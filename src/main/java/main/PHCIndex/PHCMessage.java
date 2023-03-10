package main.PHCIndex;

public class PHCMessage {
    int core;
    int CT;

    public PHCMessage(int core, int CT) {
        this.core = core;
        this.CT = CT;
    }

    public int getCore() {
        return core;
    }

    public int getCT() {
        return CT;
    }
}
