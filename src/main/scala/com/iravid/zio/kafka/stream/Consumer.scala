package com.iravid.zio.kafka.stream

import org.apache.kafka.common.TopicPartition
import com.iravid.zio.kafka.client.{ ByteRecord, Consumer, ConsumerSettings, OffsetMap, Subscription }
import scalaz.zio.{ Chunk, IO, Managed, Promise, Queue, UIO }
import scalaz.zio.stream.Stream
import scalaz.zio.duration._

object RecordStream {
  case class PartitionHandle(data: Queue[Chunk[ByteRecord]])
  object PartitionHandle {
    def make: UIO[PartitionHandle] = Queue.unbounded[Chunk[ByteRecord]].map(PartitionHandle(_))
  }

  case class Resources(
    mainOutput: Queue[(TopicPartition, Stream[Any, Nothing, ByteRecord])],
    partitions: Map[TopicPartition, PartitionHandle],
    commandQueue: Queue[Either[Poll.type, CommitRequest]]
  )

  object Resources {
    def make: UIO[Resources] =
      for {
        main <- Queue.unbounded[(TopicPartition, Stream[Any, Nothing, ByteRecord])]
        cmds <- Queue.unbounded[Either[Poll.type, CommitRequest]]
      } yield Resources(main, Map(), cmds)

  }

  case object Poll
  case class CommitRequest(offsets: OffsetMap, result: Promise[Throwable, Unit])
  trait CommitQueue {
    def commit(req: OffsetMap): IO[Throwable, Unit]
  }

  def commandLoop(consumer: Consumer, resources: Resources, pollTimeout: Duration) =
    for {
      cmd <- resources.commandQueue.take
      _ <- cmd match {
            case Left(Poll) =>
              for {
                data <- consumer.poll(pollTimeout)
                _ <- IO.foreachPar(data) {
                      case (tp, chunk) =>
                        val out = resources.partitions.get(tp) match {
                          case None         => IO.die(new Exception(s"Missing output queue for partition $tp"))
                          case Some(handle) => IO.succeed(handle)
                        }

                        out.flatMap(_.data.offer(chunk))
                    }
              } yield ()
            case Right(CommitRequest(offsets, result)) =>
              result.done(consumer.commit(offsets).provide(null))
          }
    } yield ()

  def partitioned(
    settings: ConsumerSettings,
    subscription: Subscription
  ): Stream[Any, Nothing, (TopicPartition, Stream[Any, Nothing, ByteRecord])] = {
    val _ = for {
      consumer  <- Consumer.make(settings)
      resources <- Managed.liftIO(Resources.make)
      _ <- Managed.liftIO {
            for {
              _          <- consumer.subscribe(subscription)
              assignment <- consumer.assignment
              _          <- consumer.pause(assignment)
            } yield ()
          }
      commit = (req: OffsetMap) =>
        for {
          p <- Promise.make[Throwable, Unit]
          _ <- resources.commandQueue.offer(Right(CommitRequest(req, p)))
          _ <- p.await
        } yield ()

    } yield ()

    ???
  }
}
