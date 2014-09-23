import com.twitter.conversions.time._
import com.twitter.finagle.builder.{Cluster, ClientBuilder}
import com.twitter.finagle.memcached
import com.twitter.finagle.memcached.{CacheNode, CachePoolCluster, PartitionedClient}
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import java.util.concurrent.atomic.AtomicLong
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import scala.concurrent.ExecutionContext.Implicits.{global => ec}
import com.twitter.bijection.twitter_util.UtilBijections
import com.twitter.util.{ Promise => TwitterPromise }
import scala.util.{ Success, Failure }
import shade.memcached.Codec.StringBinaryCodec

/**
 * Created by amartins on 9/22/14.
 */
object KetamaClientStress extends UtilBijections {
  private[this] case class Config(
     hosts:       String    = "mcached01-cadun.globoi.com:11211,mcached02-cadun.globoi.com:11211,mcached03-cadun.globoi.com:11211,mcached04-cadun.globoi.com:11211,mcached05-cadun.globoi.com:11211,mcached06-cadun.globoi.com:11211",
     op:          String    = "getHit",
     keysize:     Int       = 55,
     valuesize:   Int       = 1,
     numkeys:     Int       = 1,
     rwRatio:     Int       = 99, // 99% read
     loadrate:    Int       = 0,
     concurrency: Int       = 1,
     cap:         Int       = Int.MaxValue
     )
  private[this] val throughput_count = new AtomicLong
  private[this] val load_count = new AtomicLong
  private[this] val timer = DefaultTimer.twitter
  private[this] var loadTask: TimerTask = null

  def proc(op: () => Future[Any], qps: Int) {
    if (qps == 0)
      op() ensure {
        throughput_count.incrementAndGet()
        proc(op, 0)
      }
    else if (qps > 0)
      loadTask = timer.schedule(Time.now, 1.seconds) {
        1 to qps foreach { _ => op() ensure { throughput_count.incrementAndGet() }}
      }
    else
      1 to (qps * -1) foreach { _ => proc(op, 0) }
  }

  private[this] def randomString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = scala.util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }

  private[this] def createCluster(hosts: String): Cluster[CacheNode] = {
    CachePoolCluster.newStaticCluster(
      PartitionedClient.parseHostPortWeights(hosts) map {
        case (host, port, weight) => new CacheNode(host, port, weight)
      } toSet)
  }

  def toTwitterFuture(future: concurrent.Future[Any]): com.twitter.util.Future[Any] = {
    val promisse = new TwitterPromise[Any]()
    future.onComplete {
      case Success(value) => promisse.setValue(value)
      case Failure(exception) => promisse.setException(exception)
    }
    promisse
  }

  def main(args: Array[String]) {
    val config = new Config

    // the client builder
    val builder = ClientBuilder()
      .name("ketamaclient")
      .codec(Memcached())
      .failFast(false)
      .hostConnectionCoresize(config.concurrency)
      .hostConnectionLimit(config.concurrency)

    println(builder)

    // the test keys/values
    val keyValueSet: Seq[(String, ChannelBuffer)] = 1 to config.numkeys map { _ =>
      (randomString(config.keysize), ChannelBuffers.wrappedBuffer(randomString(config.valuesize).getBytes)) }
    def nextKeyValue: (String, ChannelBuffer) = keyValueSet((load_count.getAndIncrement()%config.numkeys).toInt)

    val primaryPool = createCluster(config.hosts)

    val ketamaClient = memcached.KetamaClientBuilder()
      .clientBuilder(builder)
      .cachePoolCluster(primaryPool)
      .failureAccrualParams(Int.MaxValue, Duration.Top)
      .build()

//    val spyClient = new MemcachedClient(new ConnectionFactoryBuilder()
//      .setProtocol(Protocol.BINARY)
//      .setHashAlg(DefaultHashAlgorithm.KETAMA_HASH)
//      .setLocatorType(Locator.CONSISTENT)
//      .setFailureMode(FailureMode.Redistribute)
//      .setDaemon(true)
//      .setOpTimeout(1000)
//      .setUseNagleAlgorithm(false).build(), AddrUtil.getAddresses(config.hosts))

    val shadeClient = shade.memcached.Memcached(shade.memcached.Configuration(config.hosts), ec)

    val operation = config.op match {
      case "set" =>
        () => {
          val (key, value) = nextKeyValue
          val f = shadeClient.set(key, randomString(config.valuesize), concurrent.duration.DurationInt(1000000).seconds)
          toTwitterFuture(f)
        }
      case "getHit" =>
//        keyValueSet foreach { case (k, v) => ketamaClient.set(k, v)() }
        () => {
//          val (key, _) = nextKeyValue
          ketamaClient.get("foo")
        }

//        toTwitterFuture(shadeClient.set("foo", "foo1234", concurrent.duration.DurationInt(1000000).seconds))
//
//        () => {
//          toTwitterFuture(shadeClient.get("foo"))
//        }

      case "getMiss" =>
        keyValueSet foreach { case (k, _) => ketamaClient.delete(k)() }
        () => {
          val (key, _) = nextKeyValue
          ketamaClient.get(key)
        }
      case "gets" =>
        keyValueSet foreach { case (k, v) => ketamaClient.set(k, v)() }
        () => {
          val (key, _) = nextKeyValue
          ketamaClient.gets(key)
        }
      case "getsMiss" =>
        keyValueSet foreach { case (k, _) => ketamaClient.delete(k)() }
        () => {
          val (key, _) = nextKeyValue
          ketamaClient.gets(key)
        }
      case "getsThenCas" =>
        keyValueSet map { case (k, v) => ketamaClient.set(k, v)() }
        val casMap: scala.collection.mutable.Map[String, (ChannelBuffer, ChannelBuffer)] = scala.collection.mutable.Map()

        () => {
          val (key, value) = nextKeyValue
          casMap.remove(key) match {
            case Some((_, unique)) => ketamaClient.cas(key, value, unique)
            case None => ketamaClient.gets(key) map {
              case Some(r) => casMap(key) = r
              case None => // not expecting
            }
          }
        }
      case "add" =>
        val (key, value) = (randomString(config.keysize), ChannelBuffers.wrappedBuffer(randomString(config.valuesize).getBytes))
        () => {
          ketamaClient.add(key+load_count.getAndIncrement.toString, value)
        }
      case "replace" =>
        keyValueSet foreach { case (k, v) => ketamaClient.set(k, v)() }
        () => {
          val (key, value) = nextKeyValue
          ketamaClient.replace(key, value)
        }
    }

    proc(operation, config.loadrate)


    val elapsed = Stopwatch.start()
    while (true) {
      Thread.sleep(5000)
      val howlong = elapsed()
      val howmuch_load = load_count.get()
      val howmuch_throughput = throughput_count.get()
      assert(howmuch_throughput > 0)

      printf("load: %6d QPS, throughput: %6d QPS\n",
        howmuch_load / howlong.inSeconds, howmuch_throughput / howlong.inSeconds)

      // stop generating load
      if (howmuch_load >= config.cap && loadTask != null) {
        timer.stop()
        loadTask.cancel()
      }

      // quit the loop when all load is drained
      if (howmuch_load >= config.cap && (config.loadrate == 0 || howmuch_throughput >= howmuch_load)) {
        sys.exit()
      }
    }
  }
}
