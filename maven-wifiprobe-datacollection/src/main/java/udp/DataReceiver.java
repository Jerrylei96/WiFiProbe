package udp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;

public class DataReceiver {
    public void run(int port){
        EventLoopGroup group=new NioEventLoopGroup();
        try{
            Bootstrap boot=new Bootstrap();
            boot.group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST,true)
                    .handler(new ChannelInitializer<NioDatagramChannel>(){
                        @Override
                        protected void initChannel(NioDatagramChannel channel)throws Exception{
                            channel.pipeline().addLast(new DataReceiverHandler());


                        }
                    });
            Channel channel = boot.bind(port).sync().channel();
            System.out.println("UdpServer start success "+port);
            channel.closeFuture().await();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        finally {
            group.shutdownGracefully();
        }

    }

    public static void main(String[] args) {
        int port=6000;
        new DataReceiver().run(port);
    }
}
