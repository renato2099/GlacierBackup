package org.bg.amazon.glacier;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

public class GlacierEntry {

  public static final Logger LOG = LoggerFactory.getLogger(GlacierEntry.class);

  /**
   * Main entry point for the GlacierEntry class
   * @param args
   * conf_file
   * operation
   * vault_name
   * file_name
   */
  public static void main(String[] args) {
    Options options = createOptions();
    try {

      if (args.length < 3){
        printUsage();
        throw new GlacierException("Must provide at least three arguments.");
      }
      CommandLineParser parser = new PosixParser();
      CommandLine cmd = parser.parse(options, args);

      initialize(cmd.getOptionValue("conf_file"));

      execute(cmd.getOptionValue("op_name"), cmd.getOptionValue("vault_name"), cmd.getOptionValue("file_name", ""));
      
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  /**
   * Executes the selected command
   * @param pOpName
   * @param pVaultName
   * @param pFileName
   */
  private static void execute(String pOpName, String pVaultName, String pFileName){
    if (pOpName.equals("create"))
      LOG.info("Vault " + pVaultName + (GlacierOperations.createVault(pVaultName)?" successfully created.":" not created."));
    else if (pOpName.equals("put"))
      LOG.info(pFileName + (GlacierOperations.putArchive(pVaultName, pFileName)?" uploaded successfully.":" not uploaded."));
    else if (pOpName.equals("listVault"))
      LOG.info("Vault " + pVaultName + (GlacierOperations.list(pVaultName)?" listed successfully.":" not listed."));
    else if (pOpName.equals("deleteVault"))
      LOG.info("Vault " + pVaultName + (GlacierOperations.deleteVault(pVaultName)?" delete successfully.":" not deleted."));
    else if (pOpName.equals("getFile"))
      LOG.info("File " + pFileName + (GlacierOperations.getArchive(pVaultName, pFileName)?" retrieved successfully":" not retrieved"));
    else if (pOpName.equals("deleteFile"))
      LOG.info("File " + pFileName + (GlacierOperations.deleteArchive(pVaultName, pFileName)?" deleted successfully":" not deleted"));
    else 
      LOG.info("Command not supported.");
  }

  /**
   * Method that prints parameters needed
   */
  private static void printUsage(){
    System.out.println("Parameters needed are:");
    System.out.println("-conf_file <PathToConfFile>");
    System.out.println("-op_name <create|put|listVault|deleteVault|getFile|deleteFile>");
    System.out.println("-vault_name <VaultName>");
    System.out.println("-file_name <FileName>");
  }

  /**
   * Initializes the necessary objects to perform actions
   * @param pArgs
   */
  private static void initialize(String pConfFile){
    LOG.info("Loading properties file.");
    Properties confFile = createProps(pConfFile);
    if (confFile == null)
        throw new GlacierException("Invalid properties file given: " + pConfFile);
    GlacierOperations.initialize(createAWSCredentials(confFile.getProperty("amazon.accessKey"), confFile.getProperty("amazon.secretKey")),
                                 confFile.getProperty("amazon.glacier.region"));
  }

  /**
   * Encapsulates AWSCredentials creation
   * @param pAccessKey
   * @param pSecretKey
   * @return
   */
  private static AWSCredentials createAWSCredentials(String pAccessKey, String pSecretKey){
    if (pAccessKey == null || pAccessKey.equals("") || pSecretKey == null || pSecretKey.equals(""))
      throw new GlacierException("Invalid security tokens");
    return new BasicAWSCredentials(pAccessKey, pSecretKey);
  }

  /**
   * Loads properties file from the specified path
   * @param pPropFilePath
   * @return
   */
  @SuppressWarnings("unused")
  public static Properties createProps(String pPropFilePath) {
    try {
      Properties properties = null;
      InputStream stream = new FileInputStream(pPropFilePath);
      if(stream != null) {
        try {
          properties = new Properties();
          properties.load(stream);
          return properties;
        } finally {
          stream.close();
        }
      } else
        LOG.warn(pPropFilePath + " not found, properties will be empty.");
      return properties;
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Creates the options gotten from the command line
   * @return
   */
  @SuppressWarnings("static-access")
  private static Options createOptions() {
        Options options = new Options();
        Option confFile = OptionBuilder.withArgName("conf_file").hasArg()
                .withDescription("Specify path to the configuration file to access Amazon Glacier.").create("conf_file");
        options.addOption(confFile);

        Option opName = OptionBuilder.withArgName("op_name").hasArg()
            .withDescription("Operation which will be performed using a specific vault").create("op_name");
        options.addOption(opName);

        Option vaultName = OptionBuilder.withArgName("vault_name").hasArg()
                .withDescription("Specify vault name where operations will be made").create("vault_name");
        options.addOption(vaultName);

        Option fileName = OptionBuilder.withArgName("file_name").hasArg()
                .withDescription("File to save/retrieve from an specific vault.").create("file_name");
        options.addOption(fileName);

        return options;
    }
}
