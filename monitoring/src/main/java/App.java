import socket.Server;

public class App {
    public static void main(String[] args) throws Exception {
        int port = 6660;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        new Server(port).run();
    }
}
