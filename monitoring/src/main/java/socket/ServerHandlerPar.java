package socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import kafka.Filter;
import kafka.KafkaUtil;
import kafka.WebConsoleConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class ServerHandlerPar extends ChannelInboundHandlerAdapter {

//    private final WebConsoleConsumer webConsoleConsumer;
//
//    // private final KafkaUtil kafkaUtil = new KafkaUtil();
//
//    public ServerHandlerPar(WebConsoleConsumer webConsoleConsumer) {
//        this.webConsoleConsumer = webConsoleConsumer;
//    }
//
//    @Override
//    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        ByteBuf inBuffer = (ByteBuf) msg;
//        try {
//
//            execute(inBuffer, ctx);
////            ctx.write(msg); // (1)
////            ctx.flush(); // (2)
//            // Do something with msg
////            while (inBuffer.isReadable()) { // (1)
////                System.out.print((char) inBuffer.readByte());
////                System.out.flush();
////            }
//            // String received = inBuffer.toString(CharsetUtil.UTF_8);
//            // System.out.println("Server received: " + received);
//            // ctx.write(Unpooled.copiedBuffer("Hello " + received, CharsetUtil.UTF_8));
//        } finally {
//            // ReferenceCountUtil.release(msg);
//            inBuffer.release();
//        }
//    }
//
//    private WebConsoleConsumer.MessageHandler handler = null;
//
//    public void execute(ByteBuf inBuffer, ChannelHandlerContext ctx) throws Exception {
//        String received = inBuffer.toString(CharsetUtil.UTF_8);
//        if (received.startsWith("STAT")) {
//            sendString(ctx, "STATUS: " + webConsoleConsumer.stat());
//        } else if (received.startsWith("SUB ")) {
//            String[] command = received.split(" ");
//            String topic = command[1];
//            String filterString = command.length > 2 ? command[2] : null;
//            final Filter filter = filterString == null ? Filter.PASS_ALL : Filter.getContainsFilter(filterString);
//            handler = new WebConsoleConsumer.MessageHandler() {
//                @Override
//                public void handle(ConsumerRecord<Long, String> record) {
//                    String msg = record.value();
//                    if (filter.passes(msg)) {
//                        sendString(ctx, record.topic() + "|" + msg);
//                    }
//                }
//            };
//            webConsoleConsumer.addHandler(topic, handler);
//            // sendString(ctx, "SUCCESS: subscribed to topic - " + topic);
//        } else if (received.startsWith("UNSUB ")) {
//            String[] command = received.split(" ");
//            String topic = command[1];
//            if (handler != null) webConsoleConsumer.removeHandler(topic, handler.id);
//        } else {
//            sendString(ctx, "ERROR: command not identified - " + received);
//        }
//    }
//
//    public static void sendString(ChannelHandlerContext ctx, String string) {
//        ctx.write(Unpooled.copiedBuffer(string, CharsetUtil.UTF_8));
//        ctx.flush();
//    }
//
//
////
////    @Override
////    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
////        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
////                .addListener(ChannelFutureListener.CLOSE);
////    }
//
//    @Override
//    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//        cause.printStackTrace();
//        ctx.close();
//    }
}
