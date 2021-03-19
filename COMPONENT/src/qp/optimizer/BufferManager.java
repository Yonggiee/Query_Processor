/**
 * simple buffer manager that distributes the buffers equally among all the join operators
 **/

package qp.optimizer;

public class BufferManager {

    static int numBuffer;
    static int numJoin;

    static int buffPerJoin;

    public BufferManager(int numBuffer, int numJoin) {
        this.numBuffer = numBuffer;
        this.numJoin = numJoin;
    }

    public static void setnumJoin(int numJoin) {
        BufferManager.numJoin = numJoin;
        buffPerJoin = numJoin > 0 ? numBuffer / numJoin : 0;
    }

    public static int getBuffersPerJoin() {
        return buffPerJoin;
    }

    public static int getNumBuffers() {
        return numBuffer;
    }

}
