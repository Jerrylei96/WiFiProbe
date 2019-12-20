package dao;

public class MetaData {
    private String time;
    private String id;
    private String mMac;
    private String resMac;
    private String desMac;
    private String type;
    private String subType;
    private String channel;
    private String signalPower;
    private String isTricklePower;
    private String isComeFromRouter;
    private String reserved;
    private String wifiName;

    public String getIsComeFromRouter() {
        return isComeFromRouter;
    }

    public void setIsComeFromRouter(String isComeFromRouter) {
        this.isComeFromRouter = isComeFromRouter;
    }

    public MetaData(){}

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getmMac() {
        return mMac;
    }

    public void setmMac(String mMac) {
        this.mMac = mMac;
    }

    public String getResMac() {
        return resMac;
    }

    public void setResMac(String resMac) {
        this.resMac = resMac;
    }

    public String getDesMac() {
        return desMac;
    }

    public void setDesMac(String desMac) {
        this.desMac = desMac;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSubType() {
        return subType;
    }

    public void setSubType(String subType) {
        this.subType = subType;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getSignalPower() {
        return signalPower;
    }

    public void setSignalPower(String signalPower) {
        this.signalPower = signalPower;
    }

    public String getIsTricklePower() {
        return isTricklePower;
    }

    public void setIsTricklePower(String isTricklePower) {
        this.isTricklePower = isTricklePower;
    }

    public String getReserved() {
        return reserved;
    }

    public void setReserved(String reserved) {
        this.reserved = reserved;
    }

    public String getWifiName() {
        return wifiName;
    }

    public void setWifiName(String wifiName) {
        this.wifiName = wifiName;
    }

    public MetaData(String time, String  id, String mMac, String resMac, String desMac, String type, String subType, String channel,
                    String  signalPower, String isTricklePower, String reserved, String wifiName){
        this.time = time;
        this.id = id;
        this.mMac = mMac;
        this.resMac = resMac;
        this.desMac = desMac;
        this.type = type;
        this.subType=subType;
        this.channel=channel;
        this.signalPower=signalPower;
        this.isTricklePower=isTricklePower;
        this.reserved=reserved;
        this.wifiName=wifiName;
    }
}
