public sealed interface Server permits Master, Slave {

    void runServer();
}
