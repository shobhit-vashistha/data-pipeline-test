package socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import kafka.Filter;
import kafka.SingleTopicConsumer;
import kafka.WebConsoleConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;


public class ServerHandler extends ChannelInboundHandlerAdapter {

    private SingleTopicConsumer consumer;

    public ServerHandler() {

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf inBuffer = (ByteBuf) msg;
        try {
            execute(inBuffer, ctx);
        } finally {
            // ReferenceCountUtil.release(msg);
            inBuffer.release();
        }
    }

    public void execute(ByteBuf inBuffer, ChannelHandlerContext ctx) throws Exception {
        String received = inBuffer.toString(CharsetUtil.UTF_8);
        received = received.strip();
        // return if empty msg
        if (received.length() == 0) return;

        if (received.startsWith("SUB ")) {
            String[] command = received.split(" ");
            String topic = command[1].strip();
            String filterString = command.length > 2 ? command[2].strip() : null;
            final Filter filter = filterString == null ? Filter.PASS_ALL : Filter.getContainsFilter(filterString);
            try {
                consumer = new SingleTopicConsumer();
                consumer.subscribe(topic, new SingleTopicConsumer.MessageHandler() {
                    @Override
                    public void handle(ConsumerRecord<Long, String> record) {
                        String msg = record.value();
                        if (filter.passes(msg)) {
                            sendString(ctx, msg);
                        }
                    }
                });
                consumer.start();
            } catch (KafkaException e) {
                e.printStackTrace();
                sendString(ctx, "ERROR: kafka error - " + received);
            } catch (Exception e) {
                e.printStackTrace();
                sendString(ctx, "ERROR: other error - " + received);
            } finally {
                if (consumer != null) {
                    try {
                        consumer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            sendString(ctx, "ERROR: command not identified - " + received);
        }
    }

    public static void sendString(ChannelHandlerContext ctx, String string) {
        ctx.write(Unpooled.copiedBuffer(string + "\n", CharsetUtil.UTF_8));
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        consumer.stop();
        ctx.close();
    }
}
