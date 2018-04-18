import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;

public class EpollStallTest {
    private static class StallTestHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HttpResponse) {
                HttpResponse response = (HttpResponse) msg;
                System.out.println("Got HTTP response with status " + response.status());
            }

            if (msg instanceof LastHttpContent) {
                System.out.println("finished reading response content");
            }

            super.channelRead(ctx, msg);
        }
    }

    private static class AsynchronousFileWriter {
        private static final int CHUNK_SIZE = 8192;
        private final Channel ch;
        private final AsynchronousFileChannel fc;
        private final long size;

        private AsynchronousFileWriter(Channel ch, AsynchronousFileChannel fc, long size) {
            this.ch = ch;
            this.fc = fc;
            this.size = size;
        }

        private final CompletionHandler<Integer, WriteContext> onReadComplete = new CompletionHandler<Integer, WriteContext>() {
            @Override
            public void completed(Integer result, WriteContext context) {
                if (result != CHUNK_SIZE) {
                    System.out.println(result);
                }
                context.buf.writerIndex(result);
                ch.writeAndFlush(context.buf).addListener(future -> {
                    if (!future.isSuccess()) {
                        future.cause().printStackTrace();
                    } else if (context.position + CHUNK_SIZE < size) {
                        readFile(context.position + CHUNK_SIZE);
                    } else {
                        System.out.println("finished writing request");
                        ch.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
                    }
                });
            }

            @Override
            public void failed(Throwable exc, WriteContext context) {
                exc.printStackTrace();
            }
        };

        void start() {
            readFile(0);
        }

        private void readFile(long position) {
            ByteBuf buf = Unpooled.wrappedBuffer(ByteBuffer.allocate(CHUNK_SIZE));
            ByteBuffer nioBuf = buf.nioBuffer();
            fc.read(nioBuf, position, new WriteContext(buf, position), onReadComplete);
        }

        private static class WriteContext {
            private final ByteBuf buf;
            private final long position;

            private WriteContext(ByteBuf buf, long position) {
                this.buf = buf;
                this.position = position;
            }
        }
    }

    @Test
    public void test() throws Exception {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(new EpollEventLoopGroup());
        bootstrap.channel(EpollSocketChannel.class);
        bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ch.pipeline().addLast("HttpResponseDecoder", new HttpResponseDecoder());
                ch.pipeline().addLast("HttpRequestEncoder", new HttpRequestEncoder());
                ch.pipeline().addLast("StallTestHandler", new StallTestHandler());
            }
        });

        AsynchronousFileChannel fc = AsynchronousFileChannel.open(Paths.get(System.getenv("TEMP_FILE")));
        long size = fc.size();

        ChannelFuture future = bootstrap.connect("localhost", 8080).sync();
        Channel channel = future.channel();
        channel.write(new DefaultHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.PUT,
                "http://localhost/put",
                new DefaultHttpHeaders()
                        .add(HttpHeaderNames.HOST, "localhost")
                        .add(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
                        .add(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(size))));

        new AsynchronousFileWriter(channel, fc, size).start();
        future.channel().closeFuture().sync();
    }
}
