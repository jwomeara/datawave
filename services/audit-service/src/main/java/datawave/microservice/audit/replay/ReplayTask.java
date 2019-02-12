package datawave.microservice.audit.replay;

public class ReplayTask implements Runnable {

    private ReplayStatus status;

    public ReplayTask(ReplayStatus status) {
        this.status = status;
    }

    @Override
    public void run() {
        long i = 0;
        while (i++ >= 0) {
            // do nothing
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
