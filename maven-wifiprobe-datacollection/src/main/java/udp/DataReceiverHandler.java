package udp;

import dao.MetaData;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import kafka.ProducerFactory;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.StringTokenizer;
import java.util.logging.Logger;

public class DataReceiverHandler extends SimpleChannelInboundHandler<DatagramPacket> {
    private static final Logger logger= Logger.getLogger(DataReceiverHandler.class.getName());
    private  byte[] echo ;
    private Producer producer;

    private String currentTime(){
        DateTimeFormatter formatter= DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        LocalDateTime currentTime= LocalDateTime.now();
        return currentTime.format(formatter);

    }

    public DataReceiverHandler(){
        echo=new byte[1];
        echo[0]=(byte)1;
        producer= ProducerFactory.getProducer();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet)throws Exception{
        final ByteBuf buf=packet.content();
        int readableBytes=buf.readableBytes();
        byte[] content=new byte[readableBytes];
        buf.readBytes(content);
        String clientMsg=new String(content,"UTF-8");
        StringTokenizer token=new StringTokenizer(clientMsg,"|");
        String[] result=new String[token.countTokens()];
        int i=0;
        while(token.hasMoreElements()){
            result[i++]=token.nextToken();
        }
        if(result[8].equals("0")){
            StringBuilder message=new StringBuilder();
            message.append("{\"time\":\"")
                    .append(currentTime())
                    .append("\",\"id\":\"")
                    .append("095629")
                    .append("\",\"mMac\":\"")
                    .append(result[0])
                    .append("\",\"resMac\":\"")
                    .append(result[1])
                    .append("\",\"desMac\":\"")
                    .append(result[2])
                    .append("\",\"type\":\"")
                    .append(result[3])
                    .append("\",\"subType\":\"")
                    .append(result[4])
                    .append("\",\"channel\":\"")
                    .append(result[5])
                    .append("\",\"signalPower\":\"")
                    .append(result[6])
                    .append("\",\"isTricklePower\":\"")
                    .append(result[7])
                    .append("\",\"isComeFromRouter\":\"")
                    .append(result[8])
                    .append("\",\"reserved\":\"")
                    .append(result[9])
                    .append("\",\"wifiName\":\"")
                    .append(result[10].trim())
                    .append("\"}");
            producer.send(new KeyedMessage<Integer,String>("t1",message.toString()));
            System.out.println(clientMsg.toString());
        }


        echo[0]=(byte)1;
        DatagramPacket resp=new DatagramPacket(Unpooled.copiedBuffer(echo),packet.sender());
        ctx.writeAndFlush(resp);
    }
}
