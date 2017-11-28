package forum

import io.aerospike4s.AerospikeManager

object ForumApp extends App {

  val client = AerospikeManager("localhost", 3000)

  import ForumIO._
  import io.aerospike4s._, syntax._

  val program = for {
    user <- User.createUser("romain", "lebalifant", "foo.bar@gmail.com")
    forum <- Forum.createForum("Scala")
    thread <- Forum.createThread(forum.id, "Should i use IO monad in my code?")
    _ <- Forum.createMessageOnThread("yes, you have to.", forum.id, thread.id, user.id)
    updatedThread <- Forum.getThread(forum.id, thread.id)
  } yield updatedThread
  val future = program.runFuture(client)

  future.foreach(println)
  future.failed.foreach(_.printStackTrace())
  future.onComplete(_ => client.close()) //don't forget to close client
}
