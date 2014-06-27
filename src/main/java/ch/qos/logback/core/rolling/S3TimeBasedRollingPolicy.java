package ch.qos.logback.core.rolling;


import ch.qos.logback.core.rolling.helper.CompressionMode;
import ch.qos.logback.core.rolling.helper.RenameUtil;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;

import java.io.File;
import java.util.concurrent.*;

/**
 * Extension of TimeBasedRollingPolicy.
 * <p/>
 * On each rolling event (which is defined by <triggeringPolicy>), this policy does:
 * 1. Regular log file rolling as TimeBasedRollingPolicy does but synchronously
 * 2. Upload the rolled log file to S3 bucket
 * <p/>
 * Also, this policy uploads the active log file on JVM exit. If rollingOnExit is true,
 * another log rolling happens and a rolled log is uploaded. If rollingOnExit is false,
 * the active file is directly uploaded.
 * <p/>
 * If rollingOnExit is false and if no rolling happened before JVM exits, this rolling
 * policy uploads the active log file as it is.
 */
public class S3TimeBasedRollingPolicy extends TimeBasedRollingPolicy {

  ExecutorService executor = Executors.newFixedThreadPool(1);

  String awsAccessKey;
  String awsSecretKey;
  String s3BucketName;
  String s3FolderName;
  RenameUtil renameUtil = new RenameUtil();

  boolean rollingOnExit = true;

  AmazonS3Client s3Client;

  protected AmazonS3Client getS3Client() {
    if (s3Client == null) {
      if (getAwsAccessKey() == null || getAwsSecretKey() == null) {
        s3Client = new AmazonS3Client();
      } else {
        AWSCredentials cred = new BasicAWSCredentials(getAwsAccessKey(), getAwsSecretKey());
        s3Client = new AmazonS3Client(cred);
      }
    }
    return s3Client;
  }

  @Override
  public void start() {
    super.start();
    // add a hook on JVM shutdown
    renameUtil.setContext(this.context);
    Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHookRunnable()));
  }

  @Override
  Future renamedRawAndAsyncCompress(String nameOfCompressedFile, String innerEntryName) throws RolloverFailure {
    String parentsRawFile = getParentsRawFileProperty();
    String tmpTarget = parentsRawFile + System.nanoTime() + ".tmp";
    renameUtil.rename(parentsRawFile, tmpTarget);
    Future future = asyncCompress(tmpTarget, nameOfCompressedFile, innerEntryName);
    try {
      future.get(5, TimeUnit.SECONDS);
      if (getCompressionMode() == CompressionMode.GZ){
        if(nameOfCompressedFile.endsWith(".gz")) nameOfCompressedFile = nameOfCompressedFile.substring(0, nameOfCompressedFile.lastIndexOf(".gz"));
        nameOfCompressedFile = nameOfCompressedFile + compressedFileNameSufix() + ".gz";
      } else if (getCompressionMode() == CompressionMode.ZIP) {
        if(nameOfCompressedFile.endsWith(".zip")) nameOfCompressedFile = nameOfCompressedFile.substring(0, nameOfCompressedFile.lastIndexOf(".zip"));
        nameOfCompressedFile = nameOfCompressedFile + compressedFileNameSufix() + ".zip";
      }
      uploadFileToS3Async(nameOfCompressedFile);
    } catch (TimeoutException e) {
      addError("Timeout while waiting for compression job to finish", e);
    } catch (Exception e) {
      addError("Unexpected exception while waiting for compression job to finish", e);
    }
    return future;
  }


  protected void uploadFileToS3Async(String filename) {
    final File file = new File(filename);

    // if file does not exist or empty, do nothing
    if (!file.exists() || file.length() == 0) {
      return;
    }

    // add the S3 folder name in front if specified
    final StringBuffer s3ObjectName = new StringBuffer();
    if (getS3FolderName() != null) {
      s3ObjectName.append(getS3FolderName()).append("/");
    }
    s3ObjectName.append(file.getName());

    addInfo("Uploading " + filename);
    Runnable uploader = new Runnable() {
      @Override
      public void run() {
        try {
          getS3Client().putObject(getS3BucketName(), s3ObjectName.toString(), file);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    };
    executor.execute(uploader);
  }

  // On JVM exit, upload the current log
  class ShutdownHookRunnable implements Runnable {

    @Override
    public void run() {
      try {
        if (isRollingOnExit())
          // do rolling and upload the rolled file on exit
          rollover();
        else
          // upload the active log file without rolling
          uploadFileToS3Async(getActiveFileName());

        // wait until finishing the upload
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);
      } catch (Exception ex) {
        addError("Failed to upload a log in S3", ex);
        executor.shutdownNow();
      }
    }

  }


  public String getAwsAccessKey() {
    return awsAccessKey;
  }

  public void setAwsAccessKey(String awsAccessKey) {
    this.awsAccessKey = awsAccessKey;
  }

  public String getAwsSecretKey() {
    return awsSecretKey;
  }

  public void setAwsSecretKey(String awsSecretKey) {
    this.awsSecretKey = awsSecretKey;
  }

  public String getS3BucketName() {
    return s3BucketName;
  }

  public void setS3BucketName(String s3BucketName) {
    this.s3BucketName = s3BucketName;
  }

  public String getS3FolderName() {
    return s3FolderName;
  }

  public void setS3FolderName(String s3FolderName) {
    this.s3FolderName = s3FolderName;
  }

  public boolean isRollingOnExit() {
    return rollingOnExit;
  }

  public void setRollingOnExit(boolean rollingOnExit) {
    this.rollingOnExit = rollingOnExit;
  }

  private String compressedFileNameSufix(){ return ""; }
}
