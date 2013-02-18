package org.bg.amazon.glacier;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.CreateVaultRequest;
import com.amazonaws.services.glacier.model.CreateVaultResult;
import com.amazonaws.services.glacier.model.DeleteArchiveRequest;
import com.amazonaws.services.glacier.model.DeleteVaultRequest;
import com.amazonaws.services.glacier.model.DescribeVaultRequest;
import com.amazonaws.services.glacier.model.DescribeVaultResult;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.model.ResourceNotFoundException;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

public class GlacierOperations {

  /**
   * Object containing AmazonGlacier client
   */
  private static AmazonGlacierClient client;

  /**
   * Objects to handle Amazon SQS
   */
  public static AmazonSQSClient sqsClient;
  public static String sqsQueueName = "GlacierQueue4ListOperations";
  public static String sqsQueueARN;
  public static String sqsQueueURL;

  /**
   * Object containing Amazon SNS client
   */
  public static AmazonSNSClient snsClient;
  public static String snsTopicName = "GlacierNS4ListOperations";
  public static String snsTopicARN;
  public static String snsSubscriptionARN;

  /**
   * Objects used while inventoring vaults
   */
  public static String fileName = "-inventory";
  public static long sleepTime = 600; 

  /**
   * Object containing Amazon Credentials
   */
  private static AWSCredentials credentials;

  /**
   * String containing region where operations will be performed
   */
  private static String region = "us-east-1";

  /**
   * Object in charged of performing all logging operations
   */
  private static final Logger LOG = LoggerFactory.getLogger(GlacierOperations.class);

  /**
   * Path where downloaded file will be stored
   */
  private static final String LOCAL_DIRECTORY = "./";

  /**
   * Initializes the necessary objects to perform operations
   * @param pArgs
   */
  public static void initialize(AWSCredentials pAWSCredentials, String pAWSRegion){
    LOG.info("Setting AmazonWebServices credentials up.");
    credentials = pAWSCredentials;
    region = pAWSRegion;
    LOG.info("Creating AmazonGlacierClient.");
    client = new AmazonGlacierClient(credentials);
    if (client == null)
      throw new GlacierException("Error while creating client with \nAccessKey: " + credentials.getAWSAccessKeyId()
                                  + "\nSecretKey: " + credentials.getAWSSecretKey());
    client.setEndpoint("https://glacier." + region + ".amazonaws.com");
  }

  /**
   * Method for getting a single archive from Amazon Glacier
   * @param pVaultName
   * @param pFileName
   * @return
   */
  public static boolean getArchive(String pVaultName, String pFileName){
    LOG.info("Getting file " + pFileName);
    Boolean success = false;
    try {
      ArchiveTransferManager atm = new ArchiveTransferManager(client, credentials);
      atm.download(pVaultName, pFileName, new File(LOCAL_DIRECTORY + pFileName));
    } catch (Exception e)
    {
      LOG.error("Error retrieving file " + pFileName + ".");
      LOG.error(e.getMessage());
    }
    return success;
  }

  /**
   * Method used for deleting a file from a specific vault
   * @param pVaultName
   * @param pFileName
   * @return
   */
  public static boolean deleteArchive(String pVaultName, String pFileName){
    LOG.info("Getting file " + pFileName);
    Boolean success = false;
    try {
      // Delete the archive.
      client.deleteArchive(new DeleteArchiveRequest()
          .withVaultName(pVaultName)
          .withArchiveId(pFileName));
      LOG.info("Deleted archive successfully.");
      success = true;
    } catch (Exception e)
    {
      LOG.error("Error retrieving file " + pFileName + ".");
      LOG.error(e.getMessage());
    }
    return success;
  }

  /**
   * Method which lists all archives within a vault
   * @param pVaultName
   * @return
   */
  public static boolean list(String pVaultName){
    Boolean success = false;
    client = new AmazonGlacierClient(credentials);
    client.setEndpoint("https://glacier." + region + ".amazonaws.com");
    sqsClient = new AmazonSQSClient(credentials);
    sqsClient.setEndpoint("https://sqs." + region + ".amazonaws.com");
    snsClient = new AmazonSNSClient(credentials);
    snsClient.setEndpoint("https://sns." + region + ".amazonaws.com");
    
    try {
        LOG.info("Setting AmazonSQS service up.");
        setupSQS();

        LOG.info("Setting AmazonSNS service up.");
        setupSNS();

        String jobId = initiateJobRequest(pVaultName);
        LOG.info("Job has initiated with Jobid = " + jobId);
        
        success = waitForJobToComplete(jobId, sqsQueueURL);
        if (!success) { throw new Exception("Job did not complete successfully."); }
        
        LOG.info("Starting download of inventory file from vault " + pVaultName + ".");
        downloadJobOutput(jobId, pVaultName);

        LOG.info("Unsubscribing from created services.");
        cleanUp();

    } catch (Exception e) {
        LOG.error("Inventory retrieval failed while listing valut " + pVaultName + ".");
        LOG.error(e.getMessage());
    }
    return success.booleanValue();
  }

  /**
   * Method used to download the inventory obtained from a specific vault
   * @param jobId
   * @param pVaultName
   * @throws IOException
   */
  private static void downloadJobOutput(String jobId, String pVaultName) throws IOException {
    
    GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest()
        .withVaultName(pVaultName)
        .withJobId(jobId);
    GetJobOutputResult getJobOutputResult = client.getJobOutput(getJobOutputRequest);

    LOG.info("Downloading inventory to " + pVaultName + fileName);
    FileWriter fstream = new FileWriter(pVaultName + fileName);
    BufferedWriter out = new BufferedWriter(fstream);
    BufferedReader in = new BufferedReader(new InputStreamReader(getJobOutputResult.getBody()));            
    String inputLine;
    try {
        while ((inputLine = in.readLine()) != null) {
            out.write(inputLine);
        }
    }catch(IOException e) {
        LOG.error("Error downloading inventory file from " + pVaultName);
        throw new AmazonClientException("Unable to save archive", e);
    }finally{
        try {in.close();}  catch (Exception e) {}
        try {out.close();}  catch (Exception e) {}             
    }
    LOG.info("Retrieved inventory to " + pVaultName + fileName);
}

  /**
   * Cleans subscriptions up
   */
  private static void cleanUp() {
    snsClient.unsubscribe(new UnsubscribeRequest(snsSubscriptionARN));
    snsClient.deleteTopic(new DeleteTopicRequest(snsTopicARN));
    sqsClient.deleteQueue(new DeleteQueueRequest(sqsQueueURL));
  }

  /**
   * Method which helps us waiting for the job to be completed.
   * @param jobId
   * @param sqsQueueUrl
   * @return
   * @throws InterruptedException
   * @throws JsonParseException
   * @throws IOException
   */
  private static Boolean waitForJobToComplete(String jobId, String sqsQueueUrl) throws InterruptedException, JsonParseException, IOException {
    
    Boolean messageFound = false;
    Boolean jobSuccessful = false;
    ObjectMapper mapper = new ObjectMapper();
    JsonFactory factory = mapper.getJsonFactory();
    
    while (!messageFound) {
        List<Message> msgs = sqsClient.receiveMessage(
           new ReceiveMessageRequest(sqsQueueUrl).withMaxNumberOfMessages(10)).getMessages();

        if (msgs.size() > 0) {
            for (Message m : msgs) {
                JsonParser jpMessage = factory.createJsonParser(m.getBody());
                JsonNode jobMessageNode = mapper.readTree(jpMessage);
                String jobMessage = jobMessageNode.get("Message").getTextValue();

                JsonParser jpDesc = factory.createJsonParser(jobMessage);
                JsonNode jobDescNode = mapper.readTree(jpDesc);
                String retrievedJobId = jobDescNode.get("JobId").getTextValue();
                String statusCode = jobDescNode.get("StatusCode").getTextValue();
                if (retrievedJobId.equals(jobId)) {
                    messageFound = true;
                    if (statusCode.equals("Succeeded")) {
                        jobSuccessful = true;
                    }
                }
            }
        } else {
          Thread.sleep(sleepTime * 1000);
          LOG.info("Waiting for another " + sleepTime/60 + " extra minutes");
        }
      }
    return (messageFound && jobSuccessful);
  }

  /**
   * Method that initiates the inventory job
   * @param pVaultName
   * @return
   */
  private static String initiateJobRequest(String pVaultName) {
    /** Setting parameters for inventory retrieval using the notification service */
    JobParameters jobParameters = new JobParameters()
        .withType("inventory-retrieval")
        .withSNSTopic(snsTopicARN);

    /** Creating job initiator request */
    InitiateJobRequest request = new InitiateJobRequest()
        .withVaultName(pVaultName)
        .withJobParameters(jobParameters);

    /** Obtaining job response */
    InitiateJobResult response = client.initiateJob(request);

    /** Returns job response id */
    return response.getJobId();
  }

  /**
   * Method for setting up Amazon SNS
   */
  private static void setupSNS() {
    /** Creating notification create request */
    CreateTopicRequest request = new CreateTopicRequest().withName(snsTopicName);
    CreateTopicResult result = snsClient.createTopic(request);
    snsTopicARN = result.getTopicArn();

    /** Creating a subscription request for the notification service */
    SubscribeRequest request2 = new SubscribeRequest()
        .withTopicArn(snsTopicARN)
        .withEndpoint(sqsQueueARN)
        .withProtocol("sqs");
    SubscribeResult result2 = snsClient.subscribe(request2);

    /** Obtaining the subscription result */
    snsSubscriptionARN = result2.getSubscriptionArn();
  }

  /**
   * Method to setup Amazon SQS service
   */
  private static void setupSQS() {
    /** Creating request for starting the queue */
    CreateQueueRequest request = new CreateQueueRequest().withQueueName(sqsQueueName);
    CreateQueueResult result = sqsClient.createQueue(request);  
    sqsQueueURL = result.getQueueUrl();

    /** Getting request attributes from createdqueue */
    GetQueueAttributesRequest qRequest = new GetQueueAttributesRequest()
        .withQueueUrl(sqsQueueURL)
        .withAttributeNames("QueueArn");
    
    GetQueueAttributesResult qResult = sqsClient.getQueueAttributes(qRequest);
    sqsQueueARN = qResult.getAttributes().get("QueueArn");

    /** Setting policies for performing actions inside the queue */
    Policy sqsPolicy = 
        new Policy().withStatements(
                new Statement(Effect.Allow)
                .withPrincipals(Principal.AllUsers)
                .withActions(SQSActions.SendMessage)
                .withResources(new Resource(sqsQueueARN)));
    Map<String, String> queueAttributes = new HashMap<String, String>();
    queueAttributes.put("Policy", sqsPolicy.toJson());

    /** Setting queue with the configured attributes */
    sqsClient.setQueueAttributes(new SetQueueAttributesRequest(sqsQueueURL, queueAttributes)); 
  }

  /**
   * Method which puts an archive in a single operation
   * TODO A multi-part upload should be created
   * @param pVaultName
   * @param pFilePath
   * @return
   */
  public static boolean putArchive(String pVaultName, String pFilePath){
    String pFileName = pFilePath.substring(pFilePath.lastIndexOf("/")+1, pFilePath.length());
    boolean uploadResult = false;

    if (getVaultDescription(pVaultName) == null)
      createVault(pVaultName);

    LOG.info("Uploading " + pFileName + " in a single operation");
    try {
      ArchiveTransferManager atm = new ArchiveTransferManager(client, credentials);
      UploadResult result = atm.upload(pVaultName, pFileName + (new Date()), new File(pFilePath));
      LOG.info("Archive ID: " + result.getArchiveId());
      uploadResult = true;
    } catch (Exception e){
      throw new GlacierException(e.getMessage());
    }

    return uploadResult;
  }

  /**
   * Method to create a vault with a specific name
   * @param pVaultName
   * @return
   */
  public static boolean createVault(String pVaultName) {
    boolean success = false;
    LOG.info("Creating vault " + pVaultName);
    CreateVaultRequest createVaultRequest = new CreateVaultRequest().withVaultName(pVaultName);
    CreateVaultResult createVaultResult = client.createVault(createVaultRequest);
    if (createVaultResult != null){
      LOG.info("Created vault successfully: " + describeVault(pVaultName));
      success = true;
    }
    return success;
  }

  /**
   * Method that deletes an specific vault
   * @param pVaultName
   * @return
   */
  public static boolean deleteVault(String pVaultName) {
    boolean success = false;
    try {
      DeleteVaultRequest request = new DeleteVaultRequest().withVaultName(pVaultName);
      client.deleteVault(request);
      LOG.info("Deleted vault: " + pVaultName);
      success = true;
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
    return success;
  }

  /**
   * Method to describe an existing vault
   * @param pVaultName
   * @return
   */
  public static String describeVault(String pVaultName) {
    DescribeVaultResult describeVaultResult = getVaultDescription(pVaultName);
    return  "CreationDate: " + describeVaultResult.getCreationDate() +
            "\nLastInventoryDate: " + describeVaultResult.getLastInventoryDate() +
            "\nNumberOfArchives: " + describeVaultResult.getNumberOfArchives() + 
            "\nSizeInBytes: " + describeVaultResult.getSizeInBytes() + 
            "\nVaultARN: " + describeVaultResult.getVaultARN() + 
            "\nVaultName: " + describeVaultResult.getVaultName();//createVaultResult.getLocation()
  }

  /**
   * Method that returns the vault description of an specific vault name
   * @param pVaultName
   * @return
   */
  public static DescribeVaultResult getVaultDescription(String pVaultName){
    DescribeVaultResult describeVaultResult = null;
    DescribeVaultRequest describeVaultRequest = new DescribeVaultRequest().withVaultName(pVaultName);
    try{
      describeVaultResult = client.describeVault(describeVaultRequest);
    } catch(ResourceNotFoundException re){
      LOG.error("Vault " + pVaultName + " was not found. Please check its name.");
    }
    return describeVaultResult;
  }
}
