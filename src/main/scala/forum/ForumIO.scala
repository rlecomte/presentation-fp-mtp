package forum

object ForumIO {

  import io.aerospike4s._, connection._

  val Namespace = "test"

  type UserId = String
  type ThreadId = String
  type MessageId = String
  type ForumId = String

  case class User(id: UserId, name: String, pseudo: String, mail: String)
  case class Thread(id: ThreadId, theme: String, messages: Seq[Message])
  case class Message(id: MessageId, content: String, userId: String)
  case class Forum(id: ForumId, name: String, threads: Seq[Thread])

  object User {
    val Set = keydomain(Namespace, "user")

    def updateProfile(id: UserId, pseudo: String, mail: String): AerospikeIO[User] = {
      for {
        optUser <- get[User](Set(id))
        updatedUser <- optUser match {
          case Some(user) =>
            val uu = user.copy(pseudo = pseudo, mail = mail)
            put(Set(user.id), uu).map(_ => uu)
          case None =>
            AerospikeIO.failed(new RuntimeException(s"User $id not found."))
        }
      } yield updatedUser
    }

    def createUser(name: String, pseudo: String, mail: String): AerospikeIO[User] = {
      val newUser = User(createId(), name, pseudo, mail)
      put(Set(newUser.id), newUser).map(_ => newUser)
    }

    def userExistOrFail(userId: UserId): AerospikeIO[Unit] = {
      for {
        exist <- exists(Set(userId))
        _ <- {
          if (exist) AerospikeIO.successful(())
          else AerospikeIO.failed(new RuntimeException(s"User $userId not found."))
        }
      } yield ()
    }
  }


  object Forum {
    val Set = keydomain(Namespace, "forum")

    def createForum(name: String): AerospikeIO[Forum] = {
      val newForum = Forum(createId(), name = name, threads =  Nil)
      put[Forum](Set(newForum.id), newForum).map(_ => newForum)
    }

    def createThread(idForum: ForumId, theme: String): AerospikeIO[Thread] = {
      for {
        optForum <- get[Forum](Set(idForum))
        createdThread <- optForum match {
          case Some(forum) =>
            val newThread = Thread(createId(), theme = theme, messages = Nil)
            put(Set(idForum), forum.copy(threads = forum.threads :+ newThread)).map(_ => newThread)
          case None =>
            AerospikeIO.failed(new RuntimeException(s"Forum $idForum not found."))
        }
      } yield createdThread
    }

    def createMessageOnThread(content: String, idForum: ForumId, threadId: ThreadId, userId: UserId): AerospikeIO[Message] = {

      val message = Message(createId(), content, userId)

      for {
        _ <- User.userExistOrFail(userId)
        forumOpt <- get[Forum](Set(idForum))
        updatedForum <- forumOpt match {
          case Some(forum) =>
            val threadsUpdated = forum.threads.map { thread =>
              if (thread.id == threadId) thread.copy(messages = message +: thread.messages)
              else thread
            }
            AerospikeIO.successful(forum.copy(threads = threadsUpdated))
          case None =>
            AerospikeIO.failed(new RuntimeException("Error when writing message."))
        }
        _ <- put[Forum](Set(idForum), updatedForum)
      } yield message
    }

    def getThread(idForum: ForumId, threadId: ThreadId): AerospikeIO[Thread] = {
      for {
        forumOpt <- get[Forum](Set(idForum))
        thread <- forumOpt.flatMap(forum => forum.threads.find(_.id == threadId)) match {
          case Some(thread) => AerospikeIO.successful(thread)
          case _ => AerospikeIO.failed(new RuntimeException(s"Thread $threadId not found."))
        }
      } yield thread
    }
  }

  def createId(): String = java.util.UUID.randomUUID().toString
}


