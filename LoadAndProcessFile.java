package gov.usps.mdims.interfaces;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.usps.mdims.interfaces.sywsc.DBUtilsDAO;
import gov.usps.mdims.interfaces.sywsc.WSConfigHdrInfoTO;
import gov.usps.mdims.interfaces.webtoolsapi.AddressAddInfoTO;
import gov.usps.mdims.interfaces.webtoolsapi.AddressTO;
import gov.usps.mdims.interfaces.webtoolsapi.SrcAddressTO;
import gov.usps.mdims.interfaces.webtoolsapi.WebtoolsAddrApi;
import gov.usps.mdims.utils.AlertLogUtils;
import gov.usps.mdims.utils.CommonDBMethods;
import gov.usps.mdims.utils.DBUtils;
import gov.usps.mdims.utils.DateUtils;
import gov.usps.mdims.utils.ErrorDetailsRecord;
import gov.usps.mdims.utils.ErrorHdrRecord;
import gov.usps.mdims.utils.ExceptionUtils;
import gov.usps.mdims.utils.FileUtils;
import gov.usps.mdims.utils.InterfaceUtils;
import gov.usps.mdims.utils.LogUtils;
import gov.usps.mdims.utils.ProcessErrorDtl;
import gov.usps.mdims.utils.ProcessErrorhdr;
import gov.usps.mdims.utils.StringUtils;
import oracle.jdbc.pool.OracleDataSource;
 
public class LoadAndProcessFile {
	static Logger LOGGER=LogManager.getLogger(LoadAndProcessFile.class.getName());
  
	private static final Level SYSTEM_INFO_LEVEL = Level.getLevel("MDIMS");
	
	private Connection dbConn;
	
	private LogUtils logUtils;
	private BufferedWriter logWriter; 
	private AlertLogUtils alertLogUtil;

	private FileUtils fileUtils;
	private InterfaceUtils interfaceUtils;
	private String ibErrorStatus;
	private String interfaceName;
	private int goodRecCount ;
	private int badRecCount ;
	private String errDateStr;
	private boolean recordSuccess; 
	private String currentErrorMessage;
	
	private boolean hdrPrinted;
	private boolean hdrIncludedFlag;

	private int dbLogLevel;
	private int runNumber;
	private String interfaceProcessStatus;
	private String interfaceRunMessage; 
	private String multipleErrorsInoneLinePerOrder;
	private int currentRecordNo;
	private String currentStatOrderNo; 
	private String webSrvcProcessId = null;  
	private CommonDBMethods commonDBMethods;  
	private int fileRecNumber;  
	
	public int getFileRecNumber() {
		return fileRecNumber;
	}

	public void setFileRecNumber(int fileRecNumber) {
		this.fileRecNumber = fileRecNumber;
	}

	public CommonDBMethods getCommonDBMethods() {
		return commonDBMethods;
	}

	public void setCommonDBMethods(CommonDBMethods commonDBMethods) {
		this.commonDBMethods = commonDBMethods;
	}

	public String getWebSrvcProcessId() {
		return webSrvcProcessId;
	}

	public void setWebSrvcProcessId(String webSrvcProcessId) {
		this.webSrvcProcessId = webSrvcProcessId;
	}

	public String getCurrentStatOrderNo() {
		return currentStatOrderNo;
	}

	public void setCurrentStatOrderNo(String currentStatOrderNo) {
		this.currentStatOrderNo = currentStatOrderNo;
	}

	public int getCurrentRecordNo() {
		return currentRecordNo;
	}

	public void setCurrentRecordNo(int currentRecordNo) {
		this.currentRecordNo = currentRecordNo;
	}

	public int getDbLogLevel() {
		return dbLogLevel;
	}

	public void setDbLogLevel(int dbLogLevel) {
		this.dbLogLevel = dbLogLevel;
	}

	public Connection getDbConn() {
		return dbConn;
	}

	public void setDbConn(Connection dbConn) {
		this.dbConn = dbConn;
	}

	public LogUtils getLogUtils() {
		return logUtils;
	}

	public void setLogUtils(LogUtils logUtils) {
		this.logUtils = logUtils;
	}

	public AlertLogUtils getAlertLogUtil() {
		return alertLogUtil;
	}

	public void setAlertLogUtil(AlertLogUtils alertLogUtil) {
		this.alertLogUtil = alertLogUtil;
	}

	public FileUtils getFileUtils() {
		return fileUtils;
	}

	public void setFileUtils(FileUtils fileUtils) {
		this.fileUtils = fileUtils;
	}

	public BufferedWriter getLogWriter() {
		return logWriter;
	}

	public void setLogWriter(BufferedWriter logWriter) {
		this.logWriter = logWriter;
	}

	public String getInterfaceName() {
		return interfaceName;
	}

	public void setInterfaceName(String interfaceName) {
		this.interfaceName = interfaceName;
	}

	 
	public int getGoodRecCount() {
		return goodRecCount;
	}

	public void setGoodRecCount(int goodRecCount) {
		this.goodRecCount = goodRecCount;
	}

	public int getBadRecCount() {
		return badRecCount;
	}

	public void setBadRecCount(int badRecCount) {
		this.badRecCount = badRecCount;
	}

	public boolean isRecordSuccess() {
		return recordSuccess;
	}

	public void setRecordSuccess(boolean recordSuccess) {
		this.recordSuccess = recordSuccess;
	}

	public String getCurrentErrorMessage() {
		return currentErrorMessage;
	}

	public void setCurrentErrorMessage(String currentErrorMessage) {
		this.currentErrorMessage = currentErrorMessage;
	}

	public InterfaceUtils getInterfaceUtils() {
		return interfaceUtils;
	}

	public void setInterfaceUtils(InterfaceUtils interfaceUtils) {
		this.interfaceUtils = interfaceUtils;
	}

	
	public String getIbErrorStatus() {
		return ibErrorStatus;
	}

	public void setIbErrorStatus(String ibErrorStatus) {
		this.ibErrorStatus = ibErrorStatus;
	}

	public int getRunNumber() {
		return runNumber;
	}

	
 	public String getInterfaceProcessStatus() {
		return interfaceProcessStatus;
	}

	public void setInterfaceProcessStatus(String interfaceProcessStatus) {
		this.interfaceProcessStatus = interfaceProcessStatus;
	}

 	
	public String getInterfaceRunMessage() {
		return interfaceRunMessage;
	}

	public void setInterfaceRunMessage(String interfaceRunMessage) {
		this.interfaceRunMessage = interfaceRunMessage;
	}

	public boolean isHdrPrinted() {
		return hdrPrinted;
	}

	public void setHdrPrinted(boolean hdrPrinted) {
		this.hdrPrinted = hdrPrinted;
	}

	public boolean isHdrIncludedFlag() {
		return hdrIncludedFlag;
	}

	public void setHdrIncludedFlag(boolean hdrIncludedFlag) {
		this.hdrIncludedFlag = hdrIncludedFlag;
	}

	public void setRunNumber(int runNumber) {
		this.runNumber = runNumber;
	}
	

	public String getMultipleErrorsInoneLinePerOrder() {
		return multipleErrorsInoneLinePerOrder;
	}

	public void setMultipleErrorsInoneLinePerOrder(String multipleErrorsInoneLinePerOrder) {
		this.multipleErrorsInoneLinePerOrder = multipleErrorsInoneLinePerOrder;
	}

	@SuppressWarnings("unused")
	public void runProcess(String inInterfaceName, 
						   String inDbSourceName, 
						   String inFileOpt,
						   String inOrdEntrySrc,
						   String inAddresssValidationInd,
						   String dupRecordsValidationInd ,
						   String inErrFileRequiredInd,
						   String inErrDelimiter,
						   String inMultipleErrorsInoneLinePerOrder,
						   String inErrorDateFormat,
						   String inKeyOrDateAsFirstField,
						   String inPacketNoInErrors,
						   String inZeroErrorsFileRequired
  						  ) {
		
		//116-19,116-20,116-21,116-22
		
		/*Declare Variables*/
		OracleDataSource dbSource=null;
		Timestamp programStartTime;
		String errFileNameWithPath;
		File errFile;
		//String enclosedByStr=""
		//String interfaceRunMessage = ""
		String inBoundOutboundInd;
		
	    /*Initialize variables*/ 
		 
	    setGoodRecCount(0);
		setBadRecCount(0);
		setCurrentRecordNo(0);
		setCurrentErrorMessage("");
		//setDbLogLevel(inDBLoglevel)
		//change O to I if it is inbound
	    inBoundOutboundInd="I";
	    
	    try {
	    	dbSource = DBUtils.getDataSource(inDbSourceName);
	    	setDbConn(dbSource.getConnection());
        	getDbConn().setAutoCommit(false);
            setInterfaceName(inInterfaceName);
  	    	programStartTime = new Timestamp(new java.util.Date().getTime());

  	    	errDateStr = DateUtils.getDateTimeStr("yyyyMMdd");

	    	fileUtils = new FileUtils(dbSource);
	    	fileUtils.setInterfaceName(inInterfaceName);
	    	
	    	errFileNameWithPath = fileUtils.getErrFileNameWithPath(); 
 	    	 
	    	logUtils = new LogUtils(dbSource,inInterfaceName);
	    	LOGGER.info( "Program started.");
	    	
			interfaceUtils= new InterfaceUtils() ;
			setInterfaceProcessStatus("Y");
		      

	    	interfaceUtils.setDataSource(dbSource);
	    	interfaceUtils.setLogUtils(logUtils);
	    	interfaceUtils.setDebugLevel(3);
	    	
	    	setErrorFileDelimiterValue(inErrDelimiter);
	    	setErrorFileRequired(inErrFileRequiredInd);
	    	setMultipleErrorsInoneLinePerOrder(inMultipleErrorsInoneLinePerOrder);
	    	 	    	
	    	setRunNumber(interfaceUtils.getInterfaceSeqNo());
	    	
	    	commonDBMethods = new CommonDBMethods();
	        commonDBMethods.setDbConn(getDbConn());
	    	// call actual logic for inbound and outbound programs

	    	// call 
	    	// Begin actual logic for inbound and outbound programs
	    	
	    	processFile(inFileOpt, 
	    			    inOrdEntrySrc,
	    			    inAddresssValidationInd,
	    			    inPacketNoInErrors
	    				);
	    	
	    	
	    	String retMsg = processIntRecords( inInterfaceName,inOrdEntrySrc, getRunNumber(), dupRecordsValidationInd, inPacketNoInErrors);
	    	
	    	
	    	// end of actual logic for inbound and outbound programs
	    	  
        	getDbConn().commit();
        	  
	    	if ((getBadRecCount() > 0) || ( "E".equalsIgnoreCase( getInterfaceProcessStatus()  ))){
	    		if ((getBadRecCount() > 0)) {
	    			setInterfaceRunMessage("Process completed with errors.");
	    		}
	    	} else  {
	    		setInterfaceRunMessage("Process successfully completed.");  
	    	}

	    	interfaceUtils.updateAuditInfo(inInterfaceName, 
					  						programStartTime, 
					  						getGoodRecCount()+getBadRecCount(), 
				  							getGoodRecCount(), 
				  							getBadRecCount(), 
				  							"GL", 
				  							getInterfaceRunMessage(),
				  							inBoundOutboundInd, 
				  							"B",
				  							getRunNumber());
    		  
    		  
	    	ArrayList<String> summaryOfErrors = (ArrayList<String>)logUtils.getErrorSummary();

			if ("Y".equalsIgnoreCase(multipleErrorsInoneLinePerOrder)) {
				summaryOfErrors=(ArrayList<String>)logUtils.getRecordErrorSummaryList(inErrDelimiter,inErrorDateFormat,inKeyOrDateAsFirstField);
			}
    		   
	     	alertLogUtil = new AlertLogUtils();
	    	alertLogUtil.setDataSource(dbSource);
	    	alertLogUtil.setInterfaceName(inInterfaceName);
  	    	String alertFileNameWithPath=fileUtils.getAlertFileNameWithPath();
  	    	alertLogUtil.setAlertFilePath(alertFileNameWithPath);
	     	alertLogUtil.setLogUtils(logUtils);
	    	alertLogUtil.setDebugLevel(1);
	    	alertLogUtil.setCurrentInstanceName(interfaceUtils.getCurrentInstanceName());
	    	alertLogUtil.sendEmailInterfaceError(summaryOfErrors);
	    	
	    	if (!summaryOfErrors.isEmpty()) {
	    		if ("Y".equalsIgnoreCase(errorFileRequired)) {
	    			String errorOutFileName = fileUtils.getAdditionalInterfaceFileWithPath(inInterfaceName, "DIRSETUP", "ERROUTBOUND") ;  
	    			//getOutputFilePath(getInterfaceName)   			
	    			errorOutFileName=StringUtils.getFileWithTimeStamp(errorOutFileName,null,"_");
	    			String cpCmdStr="cp "+alertFileNameWithPath+" "+errorOutFileName;	
	    			int runStatus = fileUtils.execHostCommand(cpCmdStr);
	    			LOGGER.trace("Completed writing error file.");
	    		}
	    	} else { 
	    		if ("Y".equalsIgnoreCase(errorFileRequired) && "Y".equalsIgnoreCase(inZeroErrorsFileRequired)) {
	    			String errorOutFileName = fileUtils.getAdditionalInterfaceFileWithPath(inInterfaceName, "DIRSETUP", "ERROUTBOUND") ;  
	    			//getOutputFilePath(getInterfaceName)   			
	    			errorOutFileName=StringUtils.getFileWithTimeStamp(errorOutFileName,null,"_");
	    			String cpCmdStr="cp "+alertFileNameWithPath+" "+errorOutFileName;	
	    			int runStatus = fileUtils.execHostCommand(cpCmdStr);
	    			LOGGER.trace("Completed writing zero error file.");
	    		}
	    	}
	    	LOGGER.trace("Program Ended");
	    }catch (Exception e) {
	    	LOGGER.error(ExceptionUtils.getErrorStackTrace(e, "runProcess error"));    
	    	 
	    }finally {
	    	try {
	    		if (getDbConn() !=null) {
	    			getDbConn().close();
	    		}
	    		if (logWriter!=null) {
	    			logWriter.close();
	    		}
	    	}catch(Exception e) {
	    		LOGGER.info(ExceptionUtils.getErrorStackTrace(e,"Ignore"));    
	    		 
	    	}
	    }
	}	
	
	String ibDelimiterType;
	String ibDelimiterValue;
	String ibEnlosedByValue;
	String ibDataBasedOn;
	String ibHdrIncluded;
	int    ibNoOfHdrLines;
	String errorFileDelimiterValue;
	String errorFileRequired; 
	String printFromImgTemplate; 
	String printImgTemplateRec; 
	
	public String getPrintFromImgTemplate() {
		return printFromImgTemplate;
	}

	public void setPrintFromImgTemplate(String printFromImgTemplate) {
		this.printFromImgTemplate = printFromImgTemplate;
	} 

	public String getPrintImgTemplateRec() {
		return printImgTemplateRec;
	}

	public void setPrintImgTemplateRec(String printImgTemplateRec) {
		this.printImgTemplateRec = printImgTemplateRec;
	}

	public String getIbDelimiterType() {
		return ibDelimiterType;
	}

	public void setIbDelimiterType(String ibDelimiterType) {
		this.ibDelimiterType = ibDelimiterType;
	}

	public String getIbDelimiterValue() {
		return ibDelimiterValue;
	}

	public void setIbDelimiterValue(String ibDelimiterValue) {
		this.ibDelimiterValue = ibDelimiterValue;
	}

	public String getIbEnlosedByValue() {
		return ibEnlosedByValue;
	}

	public void setIbEnlosedByValue(String ibEnlosedByValue) {
		this.ibEnlosedByValue = ibEnlosedByValue;
	} 
	
	public String getErrorFileDelimiterValue() {
		return errorFileDelimiterValue;
	}

	public void setErrorFileDelimiterValue(String errorFileDelimiterValue) {
		this.errorFileDelimiterValue = errorFileDelimiterValue;
	}

	public String getErrorFileRequired() {
		return errorFileRequired;
	}

	public void setErrorFileRequired(String errorFileRequired) {
		this.errorFileRequired = errorFileRequired;
	} 
	public String getIbDataBasedOn() {
		return ibDataBasedOn;
	}

	public void setIbDataBasedOn(String ibDataBasedOn) {
		this.ibDataBasedOn = ibDataBasedOn;
	}
	 
	public String getIbHdrIncluded() {
		return ibHdrIncluded;
	}

	public void setIbHdrIncluded(String ibHdrIncluded) {
		this.ibHdrIncluded = ibHdrIncluded;
	}

	public int getIbNoOfHdrLines() {
		return ibNoOfHdrLines;
	}

	public void setIbNoOfHdrLines(int ibNoOfHdrLines) {
		this.ibNoOfHdrLines = ibNoOfHdrLines;
	}

	@SuppressWarnings("unused")
	public void processFile(String inFileOpt, 
							String inOrdEntrySrc,
							String inAddresssValidationInd,
							String inPacketNoInErrors
							//String inDelimiterType ,
							//String inDelimiter,
				            //String inEnclosedBy,
				            //String inHdrIncludedFlag, 
  							//String inPositionOrNameBased,
  							//String inNoOfHdrLines
             				) throws SQLException {
 		File inputFile =null; 
		String inputFileNameWithPath=null;
		//String inputFilePath
		 
		ResultSet fieldDelimiterResultset = null;
		//"        nvl(ppmh.ib_delimiter_value,'|') ib_delimiter_value,"+
		String fieldDelimiterQueryStr ="SELECT nvl(ppmh.ib_delimiter_type,'CUSTOM') ib_delimiter_type,\r\n"+
										"      decode(nvl(ppmh.ib_delimiter_type,'CUSTOM'),'CUSTOM',nvl(ppmh.ib_delimiter_value,'|'),ppmh.ib_delimiter_value) ib_delimiter_value, \r\n"+
				                        "      nvl(ppmh.ib_enclosed_ind,'')ib_enclosed_ind, \r\n "+
				                        "      nvl(ppmh.ib_hdr_included,'N') ib_hdr_included, \r\n "+
				                        "      nvl(ppmh.ib_hdr_lines,0) ib_hdr_lines, \r\n "+
				                        "      nvl(ppmh.ib_data_mapped_based_on,'') ib_data_mapped_based_on, \r\n "+
				                        "      nvl(ppmh.print_from_image_template,'N') print_from_image_template \r\n "+
				                       "  FROM printable_packet_map_hdr ppmh \r\n"+
     							   	   " WHERE ppmh.process_id=? \r\n "+
				                       "   AND ppmh.application_name=? \r\n "+
       								   "   AND rownum < 2 "
       								   ; 
		
 		int fieldDelimiterCount=0;
		int nofLinestoSkip=2;
		int lineNo=0;
 		Map<String,ArrayList<InterfaceRecord>> interfaceHdrMap= new HashMap<>();
		setIbErrorStatus("N");

		boolean discardedHdr=false;
		String recordLine="";
		
		 //Web service setup
        String wsSqlStr=null;   
        ResultSet wsResultset=null;
        int wsPpmhSeqNum = 0;
        int wsMapHdrSeqId = 0;
        String webSrvcCode = null;
        String webSrvcRoot=null;        
        String printImageFinNo="";
        
        //Moved this code up
		try(PreparedStatement fieldDelimiterPreparedStmt = getDbConn().prepareStatement(fieldDelimiterQueryStr);) { 
				
				fieldDelimiterPreparedStmt.setString(1, getInterfaceName());
				fieldDelimiterPreparedStmt.setString(2, inOrdEntrySrc);
				fieldDelimiterResultset=fieldDelimiterPreparedStmt.executeQuery();
				
				LOGGER.trace("fieldDelimiterQueryStr={} ",fieldDelimiterQueryStr);   
				LOGGER.trace("fieldDelimiterQueryStr getInterfaceName()={} ",getInterfaceName());
				LOGGER.trace("fieldDelimiterQueryStr inOrdEntrySrc={} ",inOrdEntrySrc);
				
			while(fieldDelimiterResultset.next()) {
				setIbDelimiterType(fieldDelimiterResultset.getString("ib_delimiter_type"));
				setIbDelimiterValue(fieldDelimiterResultset.getString("ib_delimiter_value"));
				setIbEnlosedByValue(fieldDelimiterResultset.getString("ib_enclosed_ind"));
				setIbDataBasedOn(fieldDelimiterResultset.getString("ib_data_mapped_based_on"));
				setIbHdrIncluded(fieldDelimiterResultset.getString("ib_hdr_included"));
				setIbNoOfHdrLines(fieldDelimiterResultset.getInt("ib_hdr_lines"));
				setPrintFromImgTemplate(fieldDelimiterResultset.getString("print_from_image_template"));
				//ib_hdr_lines
				fieldDelimiterCount++ ;
				break;
			}
		}catch (Exception e) {
			fieldDelimiterCount=0;
			setIbErrorStatus("Y");
	            LOGGER.trace( ExceptionUtils.getErrorStackTrace(e, "Field Delimiter Exception"));
		}finally {  
            try {
    			if ((fieldDelimiterResultset !=null) && (!fieldDelimiterResultset.isClosed() ) ) {
    				fieldDelimiterResultset.close();
    			} 
            }catch (Exception e){
              LOGGER.error(ExceptionUtils.getErrorStackTrace(e));   
            }
		}
		
		LOGGER.trace( "delimter...   {}",getIbDelimiterValue());
		LOGGER.trace( "enclosed by...   {}",getIbEnlosedByValue());
		LOGGER.trace("Header included...   {}",getIbHdrIncluded());
		LOGGER.trace( "No Header lines...   {}",getIbNoOfHdrLines());
		//LOGGER.trace( "Header Included...   "+inHdrIncludedFlag)  
		LOGGER.trace( "Print From Image Template...   {}",getPrintFromImgTemplate());
		  
        if ("N".equalsIgnoreCase(getIbHdrIncluded()) ){
				discardedHdr=true;
		}  
        
        //check if the feature to print PS3849 forms from images is set for the application
        if ("Y".equalsIgnoreCase(getPrintFromImgTemplate())) {

        	String inputFilePath=null;
        	String stageFilePath=null;
        	String stageFileNameWithPath=null;
        	
			try { 
				
				inputFileNameWithPath = fileUtils.osFilePathConversion(fileUtils.getAdditionalInterfaceFileWithPath(getInterfaceName(), "DIRSETUP", "INBOUND"));
				inputFilePath = fileUtils.getAdditionalInterfaceFilePath();
				LOGGER.info("inputFileNameWithPath={}", inputFileNameWithPath);
				LOGGER.info("inputFilePath={}", inputFilePath);
				
				stageFileNameWithPath = fileUtils.osFilePathConversion(fileUtils.getAdditionalInterfaceFileWithPath(getInterfaceName(), "DIRSETUP", "STAGE"));
				stageFilePath = fileUtils.getAdditionalInterfaceFilePath();
				LOGGER.info("stageFileNameWithPath={}", stageFileNameWithPath);
				LOGGER.info("stageFilePath={}", stageFilePath);
				 
			}catch (Exception e) {
				LOGGER.error("Error fetching file paths  ={}", e.getMessage());
			} 
			
			if (!"".equalsIgnoreCase(StringUtils.checkNull(inputFilePath)) && !"".equalsIgnoreCase(StringUtils.checkNull(stageFilePath))) { 

				String dir = inputFilePath;
				File directory = new File(dir);
				File[] fileList = directory.listFiles();
				
				int fileCount =0;
				for (File file : fileList) {
					if( (!file.isDirectory() )) {
						fileCount++;
					}
				}
					
				
				PrintTemplateRec printTemplateRec = new PrintTemplateRec();
				
			 
				

				if (fileList.length>0) {
					//fetch the template setup in IBPS
					try {
						setPrintTemplateData( printTemplateRec, inOrdEntrySrc);  
						LOGGER.info("Print Template Set to ={}", getPrintImgTemplateRec());
						
					} catch (Exception e) {
						LOGGER.error("Exception at setPrintTemplateData ..{}", e.getMessage());
					}
				}
				
				// create interface record from template and image name 
				if (!"".equalsIgnoreCase(StringUtils.checkNull(getPrintImgTemplateRec()))) {
					
					for (File file: fileList) { 
						if( (!file.isDirectory() )) {
							LOGGER.info("Image name ={}",file.getName());
							
							try {
								printImageFinNo="";
								//int lastOccurenceOf_ = file.getName().lastIndexOf("_");
								//int lastOccurenceOfPeriod = file.getName().lastIndexOf(".");
								String finanNum=file.getName().substring(file.getName().lastIndexOf('_')+1, file.getName().lastIndexOf('.'));
								if (finanNum.length()==6) {
									printImageFinNo= finanNum;
								}
								LOGGER.info("Image Finance No ={}",printImageFinNo);
							}catch (Exception e) {
								printImageFinNo="";
							}
						
						 
							// create record line based on template
							recordLine = getPrintImgTemplateRec()+ getIbDelimiterValue() + file.getName(); 
							LOGGER.info(  "recordLine={}", recordLine);
							
							LOGGER.info(  "Processing inbound file line----------------------------------"); 
							processInterfaceRecord(recordLine,
										               inOrdEntrySrc,getIbDelimiterType(),getIbDelimiterValue(),
										               getIbEnlosedByValue(), 
										               interfaceHdrMap,
										               inPacketNoInErrors)	 ;
							
							//move the image to stage directory  
							try {
								Map.Entry<String,ArrayList<InterfaceRecord>> interfaceHdrMapLocal = interfaceHdrMap.entrySet().iterator().next();  
								ArrayList< InterfaceRecord> interfaceRecordListLocal = interfaceHdrMapLocal.getValue();
								InterfaceRecord interfaceRecordLocal =  interfaceRecordListLocal.get(0);   
								if (!"Y".equalsIgnoreCase(interfaceRecordLocal.getErrStatus())) {
									
									LOGGER.info(  "Moving the file to stage so it can be printed later by outbound ..");  
									
									printOrMove( "M", 				//inPrintMoveFlag,   
												 inputFilePath, 	//inFilePath,
												 file.getName(), 	//inFileName, 
												 stageFilePath, 	//inDestFilePath, 
												 "", 				//inDestPrinterName,
												 "N"  				//inAppendDestTstamp 
												) ;
									
									LOGGER.info(  "Done staging the file ..");
								} else {
									LOGGER.info(  "Image order creation had error and stage process is skipped. It will be reprocessed in next run .."); 
								}  
								
							} catch (Exception e) {
								LOGGER.error("Exception retrieving interface record after processInterfaceRecord");
							}
						}//if( (!file.isDirectory() )) 
					}	//for(File file: fileList) { 
					
				} //if (!"".equalsIgnoreCase(StringUtils.checkNull(getPrintImgTemplateRec()))) {
				 
			} else {
				LOGGER.info("File paths are not setup .. skipping processing images ..");
			} 
        	
        } else {
        	
        	wsSqlStr=" select distinct  ws.ppmh_seq_no, ws.web_map_hdr_seq_id, ws.web_srvc_code , wmh.process_id, wmh.root_element_tag_name \n"+
          			 "   from printable_packet_map_hdr ppmh, ibps_ws_process_info_dtl ws, webservice_map_header wmh \n"+
          			 "  where ppmh.application_name  = ? \n"+  
          			 "    and ws.ppmh_seq_no = ppmh.ppmh_seq_no \n"+
          			 "    and ws.active_flag = 'Y' \n"+
          			 "    and ws.web_srvc_code = 'ORD_GET' \n"+
          			 "    and ws.web_map_hdr_seq_id = wmh.web_map_hdr_seq_id";

       		//webservice start
               try (PreparedStatement wsPreparedStmt = getDbConn().prepareStatement(wsSqlStr, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);){

                   LOGGER.trace( "wsSqlStr MDIMS ={}",wsSqlStr);      
                   
                   wsPreparedStmt.setString(1, inOrdEntrySrc); 
                   wsResultset = wsPreparedStmt.executeQuery();
                   while (wsResultset.next()) {                         
                       wsPpmhSeqNum = wsResultset.getInt("ppmh_seq_no");
                       wsMapHdrSeqId = wsResultset.getInt("web_map_hdr_seq_id");
                       webSrvcCode = wsResultset.getString("web_srvc_code");
                       webSrvcProcessId= wsResultset.getString("process_id");
                       webSrvcRoot= wsResultset.getString("root_element_tag_name");
                       break;
                   }  
                   LOGGER.trace( "webservice setup ppmh_seq_no={} web_map_hdr_seq_id={} web_srvc_code={} ws_process_id={}", wsPpmhSeqNum, wsMapHdrSeqId, webSrvcCode, webSrvcProcessId);
               }catch(Exception e) {
               	LOGGER.error("Exception retrieving webservice setup ={}" , ExceptionUtils.getErrorStackTrace(e));
               }finally {
                   try {
                       if ((wsResultset !=null) && (!wsResultset.isClosed())) {
                       	wsResultset.close();
                       } 
                   }catch (Exception e){
                     LOGGER.error(ExceptionUtils.getErrorStackTrace(e));   
                   }
               }
        
	        try {
		        if ((webSrvcProcessId!=null &&  !"".equalsIgnoreCase(StringUtils.checkNull(webSrvcProcessId, ""))) 
		        		&& (webSrvcCode!=null &&  !"".equalsIgnoreCase(StringUtils.checkNull(webSrvcCode, "")))) {
		            LOGGER.trace( "Webservice setup exits for orders  .. retrieving Json data 1");  
		            
		            WSGetOrders wSGetOrders= new WSGetOrders(); 
		            wSGetOrders.setLogUtils(logUtils); 
		            wSGetOrders.setInterfaceUtils(interfaceUtils); 
		            wSGetOrders.setDbConn(getDbConn()); 
		            wSGetOrders.setFileUtils(fileUtils); 
		            wSGetOrders.setRunNumber(getRunNumber()); 
		            wSGetOrders.setInterfaceName(getInterfaceName()); 
		            List<String> ordRecListdata = new ArrayList<>();  
		            LOGGER.trace(  "Calling wSGetOrders.processWSGet");
		            ordRecListdata = wSGetOrders.processWSGet(webSrvcProcessId, webSrvcRoot, getIbDelimiterValue());
		            LOGGER.trace(  "After  wSGetOrders.processWSGet record cnt ={}", ordRecListdata.size());
		            
		            if(!ordRecListdata.isEmpty()) {            	
		            	for (int i = 0; i < ordRecListdata.size(); i++) {            		
		            		try {
		            			setCurrentRecordNo(getCurrentRecordNo()+1);
		            			recordLine = ordRecListdata.get(i);
		  						LOGGER.info(  "recordLine={} ",recordLine);
		  						if (discardedHdr) {
		  							LOGGER.info(  "Processing inbound WS Order line----------------------------------");
		 							
									processInterfaceRecord(recordLine,
		 									               inOrdEntrySrc,getIbDelimiterType(),getIbDelimiterValue(),
		 									               getIbEnlosedByValue(), 
		 									               interfaceHdrMap,
		 									               inPacketNoInErrors)	 ;
		 						}
		 						lineNo++;
		 						if (lineNo >= nofLinestoSkip) {
		 							if (!discardedHdr) {  
		 								//setting discardedHdr flag as true after first record in case of only one header, 
		 								//after second record in case of header details records
		 								discardedHdr=true;
		 							}
		 						}   
					        	
							} catch(Exception e){
								LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"Exception in buffer reader - "));
								
							}   
		            	} //for (int i = 0; i < ordRecListdata.size(); i++) {
		            	
		            } else {
		            	LOGGER.info("WS ordRecListdata is empty and no records to process");
		            } 
		
		    		// webservice end     
		        } else {
		        
					try {
						inputFileNameWithPath = getFileUtils().getInterfaceFileWithPath(getInterfaceName());
					}catch (Exception e) {
						LOGGER.trace("inputFileNameWithPath error ={}", e.getMessage());
					}
				
					try (FileReader inputFileReader = new FileReader(inputFileNameWithPath);){ 
						 
			 			if ("N".equalsIgnoreCase(getIbErrorStatus())) {
			 				if ("1".equalsIgnoreCase(inFileOpt)) {
			 					
			 					inputFile = new File(inputFileNameWithPath);
			 		  
			 					LOGGER.trace( "Checking if the inbound file is there...");
			 					if (inputFile.exists() && !inputFile.isDirectory()){
			 						LOGGER.trace( "File {} found!",inputFileNameWithPath );
			 					}else {
			 						LOGGER.trace("{} file not found", inputFile);
			 		   		  		setInterfaceProcessStatus("E");
			 		   		  		setInterfaceRunMessage(inputFile+" file not found");
			 						return;
			 					}  
			 					
			 					LOGGER.trace( "Checking if the inbound file is there...");
			 					LOGGER.trace("Opening file {}",inputFileNameWithPath); 
			 					
			 					try(BufferedReader bufferedReader = new BufferedReader(inputFileReader);) { 
			 					
				 					LOGGER.trace( "Opened file {}",inputFileNameWithPath);  
				 					//LOGGER(5, "input line= "+recordLine)
				 					LOGGER.info( "Processing inbound getIbDelimiterType() {}",getIbDelimiterType());
				 					LOGGER.info(  "Processing inbound getIbDelimiterValue() {}",getIbDelimiterValue());
				 					LOGGER.info(  "Processing inbound getIbEnlosedByValue() {}",getIbEnlosedByValue());
				 					LOGGER.info(  "Processing inbound getIbDataBasedOn() {}",getIbDataBasedOn());
				 					
				  					while((recordLine = bufferedReader.readLine()) != null) {
				  						setCurrentRecordNo(getCurrentRecordNo()+1);
				  						LOGGER.info(  "input line={} ",recordLine);
				  						if (discardedHdr) {
				  							LOGGER.info(  "Processing inbound file line----------------------------------");
				 							
											processInterfaceRecord(recordLine,
				 									               inOrdEntrySrc,getIbDelimiterType(),getIbDelimiterValue(),
				 									               getIbEnlosedByValue(), 
				 									               interfaceHdrMap,
				 									               inPacketNoInErrors)	 ;
				 						}
				 						lineNo++;
				 						if (lineNo >= nofLinestoSkip) {
				 							if (!discardedHdr) {  
				 								//setting discardedHdr flag as true after first record in case of only one header, 
				 								//after second record in case of header details records
				 								discardedHdr=true;
				 							}
				 						}
				 					} 
						        
			 					} catch(Exception e){
			 						LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"Exception in buffer reader - ")); 
			 					}  
			 				} 				
			 			} 
			  
					}catch (IOException e) {
						//e.printStackTrace(System.err)
						LOGGER.error(ExceptionUtils.getErrorStackTrace(e));
						 
					}catch (Exception e) {
						LOGGER.error("Ecxeption error={}", e.getMessage());
						 
					} 
		        }//if ((wsPpmhSeqNum >0) &&  wsMapHdrSeqId
	        	
	        }catch(Exception e) {
	        	LOGGER.error("Exception main error={}", e.getMessage());
	        }
        }  
        
		//stage data
		if ((interfaceHdrMap !=null ) && (interfaceHdrMap.size()>0)){
			LOGGER.info(  "calling processInterfaceData interfaceHdrMap size - {}", interfaceHdrMap.size());
			processInterfaceData(interfaceHdrMap, inPacketNoInErrors);
		}
		
		//Ship address validation in webtools  
    	try { 
    		if ("Y".equalsIgnoreCase(inAddresssValidationInd)) {
    			validateAddress(getDbConn(), inOrdEntrySrc, getRunNumber());   
        		LOGGER.trace( "Completed dmlUtilsDAO.validateAddress");              
    		}
		}catch(Exception e){
			//jsonOutput = null 
			LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"Exception validating ship address in webtools - "));
		} 
        
        LOGGER.info("End of processFile()");
 	} 

	public void processInterfaceRecord(String inInterfaceRecordStr,
			                           String inOrdEntrySrc,String inDelimiterType,String inDelimiterStr,
			                           String inEnclosedBy, 
			                           Map<String,ArrayList<InterfaceRecord>> inInterfaceHdrMap,
			                           String inPacketNoInErrors) {
		
 		String packetTypeLocal =null;
		String[] recFields=null ;
		InterfaceRecord interfaceRecord =new InterfaceRecord() ;
		interfaceRecord.setErrStatus("N");
		int processPacketTypeSeqId=0;
		fileRecNumber ++;
	  
		ResultSet packetTypeFldsResultset = null; 
 		String packetTypeFldsSqlStr="SELECT dtl.field_name_key, \r\n" + 
					                "       dtl.field_name, \r\n" + 
					                "       dtl.field_type, \r\n" + 
					                "       dtl.field_format_type, \r\n" + 
					                "       dtl.ib_nullable_ind, \r\n" + 
					                "       dtl.ib_required_ind,  \r\n" + 
					                "       dtl.tds_position, \r\n" + 
					                "       dtl.field_length, \r\n" + 
					                "       dtl.field_default_value,\r\n" + 
					                "       dtl.field_override_value,\r\n" + 
					                "       dtl.ppmd_seq_no ,\r\n" + 
					                "       hdr.packet_type,\r\n" + 
					                "       hdr.ppmh_seq_no  \r\n" + 
					                "  FROM printable_packet_map_hdr hdr, \r\n"+
					                "       printable_packet_map_dtl dtl \r\n"+
					                "  WHERE hdr.process_id=? \r\n"+
					                "    AND hdr.application_name=nvl(trim(?),'DEFAULT') \r\n" + 
					                "    AND hdr.packet_type = ?  \r\n" + 
					                "    AND dtl.ppmh_seq_no=hdr.ppmh_seq_no \r\n"  +
					                "    AND nvl(dtl.ib_active_ind,'N') = 'Y' \r\n" + 
					                "  ORDER BY dtl.tds_position asc  \r\n"  
					            ; 
		  
		ResultSet packetDestTypeResultset = null;
 		String packetDestTypeSqlStr="SELECT distinct hdr.item_no ord_item_no, \r\n"+
 				                    "       packet_no, \r\n"+
 				                    "       hdr.destination_type \r\n" + 
 			             	        "  FROM printable_forms_map_hdr hdr \r\n" + 
 			             	        " WHERE hdr.ppmh_seq_no = ? \r\n"+
 			             	        "   AND nvl(hdr.ib_active_ind,'N') = 'Y' \r\n" + 
 			             	        " ORDER BY hdr.destination_type \r\n"  
 			             	    ;
 		  
		ResultSet packetFormSeqResultset = null;
 		String packetFormSeqSqlStr="SELECT hdr.ppmh_seq_no,hdr.pfmh_seq_no, \r\n"+
 				                   "       hdr.form_type, \r\n"+
  				                   "       hdr.outbound_packet_order \r\n" + 
 			             	       "  FROM printable_forms_map_hdr hdr \r\n" + 
 			             	       " WHERE hdr.ppmh_seq_no = ? \r\n"+
 			             	       "   AND nvl(hdr.ib_active_ind,'N') = 'Y' \r\n" + 
 			             	       "   AND nvl(hdr.destination_type,'N') = ? \r\n" + 
  			             	       " ORDER BY hdr.outbound_packet_order asc \r\n"  
 			             	    ;
		 
		ResultSet packetFormDtlsResultset = null;
 		String packetFormDtlsSqlStr=" SELECT dtl.pfmd_seq_no,\r\n" + 
					                "        dtl.pfmh_seq_no,\r\n" + 
					                "        dtl.field_name,\r\n" + 
					                "        dtl.field_length,\r\n" + 
					                "        dtl.tds_position,\r\n" + 
					                "        dtl.ib_required_ind,\r\n" + 
					                "        dtl.ib_nullable_ind,\r\n" + 
					                "        dtl.outbound_header_name,\r\n" + 
					                "        dtl.outbound_seq,\r\n" + 
					                "        dtl.ack_outbound_seq,\r\n" + 
					                "        dtl.field_type,\r\n" + 
					                "        dtl.field_default_value,\r\n" + 
					                "        dtl.field_override_value,\r\n" + 
					                "        dtl.field_format_type   \r\n" + 
					                "   FROM printable_forms_map_dtl dtl\r\n" + 
					                "  WHERE dtl.pfmh_seq_no =? \r\n" + 
					                "    AND dtl.field_name IS NOT NULL\r\n" + 
					                "    AND dtl.field_length IS NOT NULL\r\n" + 
					                "    AND dtl.tds_position IS NOT NULL\r\n" + 
					                "    AND nvl(dtl.ib_active_ind,'N') = 'Y'\r\n" + 
					                "  ORDER BY dtl.tds_position asc "  
					            ;

	 	boolean multiMapNoError = true;
	 	String packetNoInErrors=null;
	 	packetTypeLocal = null; //Raj
	 	
	 	LOGGER.trace("In processInterfaceRecord");  
 		
 		if (inInterfaceRecordStr !=null) {
 			if (inDelimiterType.toLowerCase().startsWith("fixed")) {
 				// This needs to be revisited as packet type can be up to 30 characters
 				/* 
 				if (("NAOFA-AR").equalsIgnoreCase(inOrdEntrySrc)) {
 					if (inInterfaceRecordStr.length()>=27) {
 						packetTypeLocal= (inInterfaceRecordStr.substring(22,27));
 						setCurrentStatOrderNo(inInterfaceRecordStr.substring(27,43));
  					}
 				}else {
 					if (inInterfaceRecordStr.length()>=21) {
 						packetTypeLocal= (inInterfaceRecordStr.substring(16,21));
 						setCurrentStatOrderNo(inInterfaceRecordStr.substring(21,37));
  					}
 				}
 				*/
 				
 				int multiPacketMap = 0;
 				ResultSet packetStatOrdResultset = null;
 		 		String packetStatOrdSqlStr=" SELECT  distinct \r\n" +  
 							                "        dtl.field_name,\r\n" + 
 							                "        dtl.tds_position,\r\n" +  
 							                "        dtl.field_length\r\n" +  
 							                "   FROM printable_packet_map_hdr hdr,\r\n" + 
 							                "        printable_packet_map_dtl dtl\r\n" + 
 							                "  WHERE hdr.process_id=? \r\n" + 
 							                "    AND hdr.application_name=?\r\n" + 
 							                "    AND dtl.ppmh_seq_no=hdr.ppmh_seq_no \r\n" + 
 							                "    AND nvl(dtl.ib_active_ind,'N') = 'Y' \r\n" + 
 							                "    AND dtl.field_name in ('PACKGETYPE', 'CUSTOMER_ORDER_NUM')\r\n"  
 							            ;
 		 		try(PreparedStatement packetStatOrdPreparedStmt = getDbConn().prepareStatement(packetStatOrdSqlStr);) {
 		 			
 		 			packetStatOrdPreparedStmt.setString(1, getInterfaceName());
 		 			packetStatOrdPreparedStmt.setString(2, inOrdEntrySrc); 
 		 			
 	 				LOGGER.trace("packetStatOrdSqlStr={} ",packetTypeFldsSqlStr);   
 	 				LOGGER.trace("packetStatOrdSqlStr getInterfaceName()={} ",getInterfaceName());
 	 				LOGGER.trace("packetStatOrdSqlStr inOrdEntrySrc={} ",inOrdEntrySrc);  
 		 			
 	 				packetStatOrdResultset=packetStatOrdPreparedStmt.executeQuery();
 					
 					while (packetStatOrdResultset.next()) {
 						
 						int tdsPosition = packetStatOrdResultset.getInt("tds_position")-1;
 						int fieldLength = packetStatOrdResultset.getInt("field_length");
 						
 						LOGGER.trace("packetStatOrdSqlStr tdsPosition={} ",tdsPosition);
 	 	 				LOGGER.trace("packetStatOrdSqlStr fieldLength={} ",fieldLength); 
 						
 						if ("PACKGETYPE".equalsIgnoreCase(StringUtils.checkNull(packetStatOrdResultset.getString("field_name"),"xyz"))) {
 							
 							packetTypeLocal= (inInterfaceRecordStr.substring(tdsPosition,tdsPosition+fieldLength).trim()); 
 						} 
 						
 						if ("CUSTOMER_ORDER_NUM".equalsIgnoreCase(StringUtils.checkNull(packetStatOrdResultset.getString("field_name"),"xyz"))) {
 							 
 							setCurrentStatOrderNo(inInterfaceRecordStr.substring(tdsPosition,tdsPosition+fieldLength).trim());
 							 
 						}  

						multiPacketMap++;
 					} 
 		 			
	 			}catch(Exception e) {
	 				LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"packetStatOrdPreparedStmt "));
		 		}finally {
		 			try { 
		 				if ((packetStatOrdResultset !=null) && (  !packetStatOrdResultset.isClosed() ) ){
		 					packetStatOrdResultset.close();
						} 
		 			}catch(Exception e) {
	 	 				LOGGER.info(  ExceptionUtils.getErrorStackTrace(e,"Ignore"));
		 			}
	 	 		} 
 		 		 
 		 		// Making sure there is valid and unique packet map for the interface and application to fetch packet type and stat order no
 		 		if (multiPacketMap != 2) {
 		 			multiMapNoError = false;
 		 			LOGGER.error(  " Record failed due to mutiple/incorrecct header maps for same interface/source");
 		 		} 

 			}else {
 				recFields = StringUtils.splitAndTrimEnclosure(inInterfaceRecordStr,inDelimiterStr,inEnclosedBy );  
 				//inInterfaceRecordStr.split("\\"+inDelimiterStr,-1)
 				//for position based
 				if (recFields.length >=3) {
 					packetTypeLocal= recFields[2];
 				} 
 				if (recFields.length >=4) {
 					
 					if ((recFields[3] !=null) && (!"".equalsIgnoreCase(recFields[3])) ) {
 						setCurrentStatOrderNo(recFields[3]);
 					}else {
 						setCurrentStatOrderNo(getRunNumber()+"-"+fileRecNumber); 
 						recFields[3] =getCurrentStatOrderNo();
 					}
 				}
 			}
 		}
 		
 		LOGGER.info(  "packetTypeLocal - {}", packetTypeLocal);
 		LOGGER.info(  "StatOrderNo     - {}", getCurrentStatOrderNo());

 		if (packetTypeLocal !=null && !"".equalsIgnoreCase(StringUtils.checkNull(packetTypeLocal, ""))) {
			packetTypeLocal=packetTypeLocal.trim();
			
		} else {
			LOGGER.error(  " Record failed as packet type is missing.");
			String recIdentfierNo="rec"+getCurrentRecordNo();
			
			if ("Y".equalsIgnoreCase(errorFileRequired)) {
				if ("Y".equalsIgnoreCase(multipleErrorsInoneLinePerOrder)) {
					
					if (!"".equalsIgnoreCase(StringUtils.checkNull(interfaceRecord.getCustOrdNo().getFieldValue())) ){
						recIdentfierNo	=interfaceRecord.getCustOrdNo().getFieldValue();
					}else {
						if (!"".equalsIgnoreCase(StringUtils.checkNull(getCurrentStatOrderNo())) ){
							recIdentfierNo	=getCurrentStatOrderNo();
						}
					}
					logUtils.recordErrorSummary(recIdentfierNo," Packet type required Field is missing");
				}else {
					logUtils.recordErrorSummary(errDateStr+errorFileDelimiterValue+recIdentfierNo+errorFileDelimiterValue+" Packet type required Field is missing ");
				}
			}else {
				logUtils.recordErrorSummary(recIdentfierNo+" Packet type required Field is missing");
			}
		}
 		
 		if ("Y".equalsIgnoreCase(inPacketNoInErrors)) {
 			packetNoInErrors=packetTypeLocal+errorFileDelimiterValue;
 		}
 		 
 		if (packetTypeLocal !=null && !"".equalsIgnoreCase(StringUtils.checkNull(packetTypeLocal, "")) && multiMapNoError) {
 			
 			try(PreparedStatement packetTypeFldsPreparedStmt = getDbConn().prepareStatement(packetTypeFldsSqlStr);) {   
 					
 				packetTypeFldsPreparedStmt.setString(1, getInterfaceName());
	 			packetTypeFldsPreparedStmt.setString(2, inOrdEntrySrc);
	 			packetTypeFldsPreparedStmt.setString(3, packetTypeLocal); 
	 			
 				LOGGER.trace("packetTypeFldsSqlStr={} ",packetTypeFldsSqlStr);   
 				LOGGER.trace("packetTypeFldsSqlStr getInterfaceName()={} ",getInterfaceName());
 				LOGGER.trace("packetTypeFldsSqlStr inOrdEntrySrc={} ",inOrdEntrySrc);
 				LOGGER.trace("packetTypeFldsSqlStr Packet={} ",packetTypeLocal);
	 			
	 			
				packetTypeFldsResultset=packetTypeFldsPreparedStmt.executeQuery();
				while (packetTypeFldsResultset.next()) {
 					int tdsPostition;
 					int fldLength;
 					String fldNullableInd="Y";
 					String fldRequiredInd="Y";
 					String fldFormat;
 					String fldType;
 					String fldName=packetTypeFldsResultset.getString("field_name"); //
					tdsPostition=packetTypeFldsResultset.getInt("tds_position");
 					fldLength=packetTypeFldsResultset.getInt("field_length"); //
 					LOGGER.trace("tdsPostition={} ",tdsPostition);
 					LOGGER.trace("fldLength={} ",fldLength);
 					
	 				fldRequiredInd=packetTypeFldsResultset.getString("ib_required_ind"); //
 					fldNullableInd=packetTypeFldsResultset.getString("ib_nullable_ind"); //
 					if (fldRequiredInd==null) {
 						fldRequiredInd="N";
 					}
 					if (fldNullableInd==null) {
 						fldNullableInd="N";
 					}
 					fldFormat=packetTypeFldsResultset.getString("field_format_type"); //
 					fldType=packetTypeFldsResultset.getString("field_type"); //
 					int fldStartPostion=tdsPostition-1;
 					int fldEndPostion=fldStartPostion+fldLength;
 					processPacketTypeSeqId=packetTypeFldsResultset.getInt("ppmh_seq_no"); //
 					FieldAttributes fldData= new FieldAttributes();
 					
 					if (inDelimiterType.toLowerCase().startsWith("fixed")) {
	  		 			if  ( ("Y".equalsIgnoreCase(fldRequiredInd)) && (!"Y".equalsIgnoreCase(fldNullableInd)) ) {
	 						if ("".equalsIgnoreCase( StringUtils.checkNull(inInterfaceRecordStr.substring(fldStartPostion,fldEndPostion) ) )){
	 							if ((packetTypeFldsResultset.getString("field_default_value")==null) && (packetTypeFldsResultset.getString("field_override_value")==null)) {
	 								badRecCount++;
	 								interfaceRecord.setErrStatus("Y");
	 								interfaceRecord.setErrMessage("Validation failed");
	 								setProcessErrorInfo(interfaceRecord, "Missing Required Field="+fldName);
	 								interfaceRecord.setErrProcessMsg(StringUtils.checkNull(interfaceRecord.getErrProcessMsg())+" Missing Required Field="+fldName);
	 								LOGGER.trace("Interface process1 err msg ={}",interfaceRecord.getErrProcessMsg());
	 								
	 								if ("Y".equalsIgnoreCase(errorFileRequired)) {
	 									if ("Y".equalsIgnoreCase(multipleErrorsInoneLinePerOrder)) {
	 										String recIdentfierNo="rec"+getCurrentRecordNo();
	 										if (!"".equalsIgnoreCase(StringUtils.checkNull(interfaceRecord.getCustOrdNo().getFieldValue())) ){
	 											recIdentfierNo	=interfaceRecord.getCustOrdNo().getFieldValue();
	 										}else {
 	 											if (!"".equalsIgnoreCase(StringUtils.checkNull(getCurrentStatOrderNo())) ){
	 												recIdentfierNo	=getCurrentStatOrderNo();
	 											}
 	 										}
	 										logUtils.recordErrorSummary(packetNoInErrors+recIdentfierNo," Required Field is missing for "+fldName);
	 									}else {
	 										logUtils.recordErrorSummary(errDateStr+errorFileDelimiterValue+packetNoInErrors+interfaceRecord.getCustOrdNo().getFieldValue()+errorFileDelimiterValue+" Required Field is missing "+fldName);
	 									}
	 								}else {
	 									logUtils.recordErrorSummary(packetNoInErrors+interfaceRecord.getCustOrdNo().getFieldValue()+" Required Field is missing for "+fldName);
	 								}
	 							}
	 						}
	 					}
						
 						fldData.setFieldValue(inInterfaceRecordStr.substring(fldStartPostion,fldEndPostion));
 	 
	 		  		}else {
	 					if  ( ("Y".equalsIgnoreCase(fldRequiredInd)) && (!"Y".equalsIgnoreCase(fldNullableInd)) ) {
	 						if ("".equalsIgnoreCase( StringUtils.checkNull( recFields[fldStartPostion] ) )){
	 							if ((packetTypeFldsResultset.getString("field_default_value")==null) && (packetTypeFldsResultset.getString("field_override_value")==null)) {
	 								badRecCount++;
		 							interfaceRecord.setErrStatus("Y");
		 							interfaceRecord.setErrMessage("Validation failed");
		 							setProcessErrorInfo(interfaceRecord, "Missing Required Field="+fldName);
		 							interfaceRecord.setErrProcessMsg(StringUtils.checkNull(interfaceRecord.getErrProcessMsg())+" Missing Required Field="+fldName);
		 							LOGGER.trace("Interface process2 err msg ={}",interfaceRecord.getErrProcessMsg());
		 							if ("Y".equalsIgnoreCase(errorFileRequired)) {
		 								
		 								if ("Y".equalsIgnoreCase(multipleErrorsInoneLinePerOrder)) {
		 									String recIdentfierNo="rec"+getCurrentRecordNo();
		 									if (!"".equalsIgnoreCase(StringUtils.checkNull(interfaceRecord.getCustOrdNo().getFieldValue())) ){
		 										recIdentfierNo	=interfaceRecord.getCustOrdNo().getFieldValue();
 		 									}else {
		 											
		 										if (!"".equalsIgnoreCase(StringUtils.checkNull(getCurrentStatOrderNo())) ){
		 											recIdentfierNo	=getCurrentStatOrderNo();
		 										}
 		 									}
		 									
		 									
	 										logUtils.recordErrorSummary(packetNoInErrors+recIdentfierNo," Required Field is missing for "+fldName);
	 									}else {
	 										logUtils.recordErrorSummary(errDateStr+errorFileDelimiterValue+packetNoInErrors+interfaceRecord.getCustOrdNo().getFieldValue()+errorFileDelimiterValue+" Required Field is missing "+fldName);
	 									}
	 								}else {
	 									logUtils.recordErrorSummary(packetNoInErrors+interfaceRecord.getCustOrdNo().getFieldValue()+" Required Field is missing for "+fldName);
	 								}
		 							
	 							}
	 						}
	 					}
						 
 						fldData.setFieldValue(recFields[fldStartPostion]);
	 		 		}  // fixed or custom if 
 					fldData.setFieldFormatType(fldFormat);
					fldData.setFieldType(fldType);
					fldData.setFieldDefaultValue(packetTypeFldsResultset.getString("field_default_value"));
					fldData.setFieldOverWriteValue(packetTypeFldsResultset.getString("field_override_value"));
					fldData.setFieldFormatType(fldFormat);
					fldData.setFieldType(fldType);
					if ("".equalsIgnoreCase( StringUtils.checkNull(fldData.getFieldValue()) )   ){
						if (!"".equalsIgnoreCase( StringUtils.checkNull(fldData.getFieldDefaultValue()) )   ){
							fldData.setFieldValue(fldData.getFieldDefaultValue());
						}
					}
						
					if (!"".equalsIgnoreCase( StringUtils.checkNull(fldData.getFieldOverWriteValue()) )   ){
						fldData.setFieldValue(fldData.getFieldOverWriteValue());
					}
					
					LOGGER.trace("Field Name= {} Value= {}", fldName,fldData.getFieldValue());
					
					if (!getFldValueValidation(fldData)){
						fldData.setFieldValue(null);
						badRecCount++;
						interfaceRecord.setErrStatus("Y");
						interfaceRecord.setErrMessage("Validation failed");
						setProcessErrorInfo(interfaceRecord, "Field Format failure="+fldName);
						interfaceRecord.setErrProcessMsg(StringUtils.checkNull(interfaceRecord.getErrProcessMsg())+" Field Format failure="+fldName);
						LOGGER.trace("Interface process3 err msg ={}",interfaceRecord.getErrProcessMsg());
						if ("Y".equalsIgnoreCase(errorFileRequired)) {
							if ("Y".equalsIgnoreCase(multipleErrorsInoneLinePerOrder)) {
								String recIdentfierNo="rec"+getCurrentRecordNo();
								if (!"".equalsIgnoreCase(StringUtils.checkNull(interfaceRecord.getCustOrdNo().getFieldValue())) ){
									recIdentfierNo	=interfaceRecord.getCustOrdNo().getFieldValue();
								}else {
 									if (!"".equalsIgnoreCase(StringUtils.checkNull(getCurrentStatOrderNo())) ){
										recIdentfierNo	=getCurrentStatOrderNo();
									}
								}
								logUtils.recordErrorSummary(packetNoInErrors+recIdentfierNo," Field Format failure for "+fldName);
							}else {
								logUtils.recordErrorSummary(errDateStr+errorFileDelimiterValue+packetNoInErrors+interfaceRecord.getCustOrdNo().getFieldValue()+errorFileDelimiterValue+"Field Format failure for "+fldName);
							}
						}else {
							logUtils.recordErrorSummary(packetNoInErrors+interfaceRecord.getCustOrdNo().getFieldValue()+" Field Format failure for "+fldName);
						}
					}
					setPacketData(interfaceRecord, packetTypeFldsResultset.getString("field_name_key"),fldData);
 				}
				interfaceRecord.setPacketSeq(processPacketTypeSeqId);
				interfaceRecord.setApplicationName(interfaceRecord.getOrdEntrySrc().getFieldValue());
				if ((interfaceRecord.getProcessErrorhdr() !=null)&&( !interfaceRecord.getProcessErrorhdr().getProcessErrorDtlList().isEmpty())) {
                 	interfaceUtils.saveErrorRecord(interfaceRecord.getProcessErrorhdr());
                }
				if ( "".equalsIgnoreCase(StringUtils.checkNull(interfaceRecord.getOrderQty().getFieldValue()))){
					interfaceRecord.getOrderQty().setFieldValue("1") ;
				}
				
 			}catch(Exception e) {
 				LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"processInterfaceRecord1 "));
				logUtils.recordErrorSummary("Invalid format for record "+inInterfaceRecordStr);
				//logUtils.recordErrorSummary(packetNoInErrors+interfaceRecordForMap.getCustOrdNo().getFieldValue(), " Required field missing for "+packetFormDtlsResultset.getString("field_name"));
	 		}finally {
	 			try { 
	 				if ((packetTypeFldsResultset !=null) && (  !packetTypeFldsResultset.isClosed() ) ){
						packetTypeFldsResultset.close();
					} 
	 			}catch(Exception e) {
 	 				LOGGER.info(  ExceptionUtils.getErrorStackTrace(e,"Ignore"));
	 			}
 	 		}
 		
 			try(PreparedStatement packetDestTypePreparedStmt = getDbConn().prepareStatement(packetDestTypeSqlStr);) {   
 				
 				LOGGER.info(  "packetDestTypeSqlStr={} ",packetDestTypeSqlStr); 
 	 			packetDestTypePreparedStmt.setInt(1,processPacketTypeSeqId);
 	 			
 				LOGGER.trace("packetDestTypeSqlStr={} ",packetDestTypeSqlStr);   
 				LOGGER.trace("packetDestTypeSqlStr processPacketTypeSeqId={} ",processPacketTypeSeqId); 
 	 			
 	 			packetDestTypeResultset=packetDestTypePreparedStmt.executeQuery();
 				ArrayList< InterfaceRecord> interfaceRecordList = inInterfaceHdrMap.get(interfaceRecord.getCustOrdNo().getFieldValue());
 				while (packetDestTypeResultset.next()) {
 					
  					
 					InterfaceRecord interfaceRecordForMap =new InterfaceRecord() ;
 					//if !"Y".equalsIgnoreCase(interfaceRecord.getErrStatus
 						
	 	  				interfaceRecordForMap.setPacketSeq(interfaceRecord.getPacketSeq());
	 	  				interfaceRecordForMap.setEdiTpId( interfaceRecord.getEdiTpId());
	 					interfaceRecordForMap.setOrdEntrySrc(interfaceRecord.getOrdEntrySrc());
	 					interfaceRecordForMap.setPacketType(interfaceRecord.getPacketType());
	 					interfaceRecordForMap.setCustOrdNo(interfaceRecord.getCustOrdNo());
	 					interfaceRecordForMap.setCustOrdLineNo(interfaceRecord.getCustOrdLineNo());
	 					interfaceRecordForMap.setCustAttention(interfaceRecord.getCustAttention());
	 					interfaceRecordForMap.setCustAddress1(interfaceRecord.getCustAddress1());
	 					interfaceRecordForMap.setCustAddress2(interfaceRecord.getCustAddress2());
	 					interfaceRecordForMap.setCustAddress3(interfaceRecord.getCustAddress3());
	 					interfaceRecordForMap.setCustCity(interfaceRecord.getCustCity());
	 					interfaceRecordForMap.setCustState(interfaceRecord.getCustState());
	 					interfaceRecordForMap.setCustZipCode(interfaceRecord.getCustZipCode());
	 					interfaceRecordForMap.setCustCountry(interfaceRecord.getCustCountry());
	 					interfaceRecordForMap.setDlvryPoint(interfaceRecord.getDlvryPoint());
	 					interfaceRecordForMap.setCustFinanceNo(interfaceRecord.getCustFinanceNo());
	 					interfaceRecordForMap.setEmpId(interfaceRecord.getEmpId());
	 					interfaceRecordForMap.setEmpName(interfaceRecord.getEmpName());
	 					interfaceRecordForMap.setItemNo(interfaceRecord.getItemNo());
	 					interfaceRecordForMap.setOrderQty(interfaceRecord.getOrderQty());
	 					interfaceRecordForMap.setUnitCost(interfaceRecord.getUnitCost());
	 					interfaceRecordForMap.setTotalCost(interfaceRecord.getTotalCost());
	 					interfaceRecordForMap.setBillToFedstrip(interfaceRecord.getBillToFedstrip());
	 					interfaceRecordForMap.setUnitOfMeasure(interfaceRecord.getUnitOfMeasure());
	 					interfaceRecordForMap.setPriorityCode(interfaceRecord.getPriorityCode());
	 					interfaceRecordForMap.setServiceTypeCode(interfaceRecord.getServiceTypeCode());
	 					interfaceRecordForMap.setReplaceInd(interfaceRecord.getReplaceInd());
	 					interfaceRecordForMap.setBrokenPartSerialNo(interfaceRecord.getBrokenPartSerialNo());
	 					interfaceRecordForMap.setOrderDate(interfaceRecord.getOrderDate());
	 					interfaceRecordForMap.setAltCustAddress1(interfaceRecord.getAltCustAddress1());
	 					interfaceRecordForMap.setAltCustAddress2(interfaceRecord.getAltCustAddress2());
	 					interfaceRecordForMap.setAltCustAddress3(interfaceRecord.getAltCustAddress3());
	 					interfaceRecordForMap.setAltCustCity(interfaceRecord.getAltCustCity());
	 					interfaceRecordForMap.setAltCustState(interfaceRecord.getAltCustState());
	 					interfaceRecordForMap.setAltCustZipCode(interfaceRecord.getAltCustZipCode());
	 					interfaceRecordForMap.setAltCustCountry(interfaceRecord.getAltCustCountry());
	 					interfaceRecordForMap.setAltDlvryPoint(interfaceRecord.getAltDlvryPoint());
	 	 				interfaceRecordForMap.setApplicationName(interfaceRecord.getApplicationName());
	 	 				interfaceRecordForMap.setDestinationType(packetDestTypeResultset.getString("destination_type"));
	 	 				interfaceRecordForMap.setPacketNo(packetDestTypeResultset.getString("packet_no"));
	 	 				if (!"N".equalsIgnoreCase(interfaceRecord.getErrStatus())) {
	 	 					interfaceRecordForMap.setErrStatus("B");
	 	 				}else {
	 	 					interfaceRecordForMap.setErrStatus(interfaceRecord.getErrStatus());
	 	 				}
	 	 				interfaceRecordForMap.setErrProcessMsg(interfaceRecord.getErrProcessMsg()); 
	 	 				LOGGER.trace("interfaceRecordForMap Interface process err msg ={}",interfaceRecord.getErrProcessMsg());
	 	 				
	 	 				LOGGER.trace("interfaceRecordForMap CustOrdNo()={} ",interfaceRecord.getCustOrdNo());
	 	 				LOGGER.trace("interfaceRecordForMap CustOrdLineNo()={} ",interfaceRecord.getCustOrdLineNo());
	 	 				LOGGER.trace("interfaceRecordForMap packet_no={} ",packetDestTypeResultset.getString("packet_no"));
	 	 				LOGGER.trace("interfaceRecordForMap destination_type={} ",packetDestTypeResultset.getString("destination_type"));
	 	 				
	 	 				HashMap<String,ArrayList<InterfaceDetailRecord>> interfaceFormDtlsMap = new HashMap<>();
	 	 				
	 	 				try(PreparedStatement packetFormSeqPreparedStmt = getDbConn().prepareStatement(packetFormSeqSqlStr);) {   
	 	  					 	 	 					
	 	 					LOGGER.trace("packetFormSeqSqlStr={} ",packetFormSeqSqlStr);   
	 	 	 				LOGGER.trace("packetFormSeqSqlStr processPacketTypeSeqId={} ",processPacketTypeSeqId);
	 	 	 				LOGGER.trace("packetFormSeqSqlStr destination_type={} ",packetDestTypeResultset.getString("destination_type"));
	 	 	 				
	 	 					packetFormSeqPreparedStmt.setInt(1, processPacketTypeSeqId);
	 	 					packetFormSeqPreparedStmt.setString(2, packetDestTypeResultset.getString("destination_type"));
	 	 					packetFormSeqResultset=packetFormSeqPreparedStmt.executeQuery();
	 	 					
	 	 					while (packetFormSeqResultset.next()) {
	 	 				 		String formType=packetFormSeqResultset.getString("form_type");
	 	 						int pfmhSeqId=packetFormSeqResultset.getInt("pfmh_seq_no");
	 	 						LOGGER.trace("formType={} ",formType);
	 							ArrayList<InterfaceDetailRecord> interfaceDetailList = new ArrayList<>() ;
	 		 	 				
	 							try(PreparedStatement packetFormDtlsPreparedStmt = getDbConn().prepareStatement(packetFormDtlsSqlStr);) {
	 		 	 					
	 		 	 					LOGGER.trace("packetFormDtlsSqlStr={} ",packetFormSeqSqlStr);   
	 		 	 	 				LOGGER.trace("packetFormDtlsSqlStr pfmhSeqId={} ",pfmhSeqId);
	 		 	 					
	 	 	 	 					packetFormDtlsPreparedStmt.setInt(1, pfmhSeqId); 
	 	 	 	 					packetFormDtlsResultset=packetFormDtlsPreparedStmt.executeQuery();
	 	  	 	 					while (packetFormDtlsResultset.next()) {
	 	 	 	 						InterfaceDetailRecord interfaceDetailRecord = new InterfaceDetailRecord();
	 	 	 	 						interfaceDetailRecord.setFieldSeqNo(packetFormDtlsResultset.getInt("pfmd_seq_no"));
	 	 	 	 						interfaceDetailRecord.setFormSeqNo(pfmhSeqId);
	 	 	 	 						interfaceDetailRecord.setFieldName(packetFormDtlsResultset.getString("field_name"));
	 	 	 	 						interfaceDetailRecord.setFieldLength(packetFormDtlsResultset.getInt("field_length"));
	 	 	 	 						interfaceDetailRecord.setTdsPosition(packetFormDtlsResultset.getInt("tds_position"));
	 	 	 	 						interfaceDetailRecord.setIbRequiredInd(packetFormDtlsResultset.getString("ib_required_ind"));
	 	 	 	 						interfaceDetailRecord.setObHeaderName(packetFormDtlsResultset.getString("outbound_header_name"));
	 	 	 	 						interfaceDetailRecord.setOutboundSeq( packetFormDtlsResultset.getInt("outbound_seq"));
	 	 	 	 						interfaceDetailRecord.setAckOutboundSeq(packetFormDtlsResultset.getInt("ack_outbound_seq"));
	 	 	 	 						interfaceDetailRecord.setFieldType(packetFormDtlsResultset.getString("field_type"));
	 	 	 	 						interfaceDetailRecord.setFieldFormatType(packetFormDtlsResultset.getString("field_format_type"));
	 	 	 	 						interfaceDetailRecord.setFieldDefaultValue(packetFormDtlsResultset.getString("field_default_value"));
	 	 	 	 						interfaceDetailRecord.setFieldOverWriteValue(packetFormDtlsResultset.getString("field_override_value"));
	 	 	 	 						//interfaceDetailRecord.setOutbSeparatorTag(packetFormDtlsResultset.getString("outb_separator_tag" 
	 	 	 	 						//interfaceDetailRecord.setInbSeparatorTag(packetFormDtlsResultset.getString("inb_separator_tag" 
	 	 	 	 						
	 	 	 	 						if ("".equalsIgnoreCase( StringUtils.checkNull(interfaceDetailRecord.getFieldValue()) )   ){
	 	 	 	 							if (!"".equalsIgnoreCase( StringUtils.checkNull(interfaceDetailRecord.getFieldDefaultValue()) )   ){
	 	 	 	 								interfaceDetailRecord.setFieldValue(interfaceDetailRecord.getFieldDefaultValue());
	 	 	 	 							}
	 	 	 	 						}
	 	 	 	 						
	 	 	 	 						
	 	 	 	 						if (!"".equalsIgnoreCase( StringUtils.checkNull(interfaceDetailRecord.getFieldOverWriteValue()) )   ){
	 	 	 	 							interfaceDetailRecord.setFieldValue(interfaceDetailRecord.getFieldOverWriteValue());
	 	 	 	 						}
	
	 	 	 	 						
	 	 	 	 	 					int fldStartPostionDet=interfaceDetailRecord.getTdsPosition()-1;
	 	 	 		 					int fldEndPostionDet=fldStartPostionDet+interfaceDetailRecord.getFieldLength();
	
	 	 	 	 						
	 	 	 		 					if (inDelimiterType.toLowerCase().startsWith("fixed")) {
  	 	 	 	 	 						interfaceDetailRecord.setFieldValue(inInterfaceRecordStr.substring(fldStartPostionDet,fldEndPostionDet));
	 	  	 	 						}else {
	 	 	 	 	 						interfaceDetailRecord.setFieldValue(recFields[interfaceDetailRecord.getTdsPosition()-1]);
	 	  	 	 						}
	 	 	 	 						//inInterfaceRecordStr
	 	  	 	 						String fldNullableInd="Y";
	 	 	 		 					String fldRequiredInd="Y";
	 	  	 		 			  		fldRequiredInd=packetFormDtlsResultset.getString("ib_required_ind"); //
	 	 			 					fldNullableInd=packetFormDtlsResultset.getString("ib_nullable_ind"); //
	 	  	 	 	 					interfaceRecord.setErrStatus("N");
	 	 	 	 	 					if  ( ("Y".equalsIgnoreCase(fldRequiredInd)) && (!"Y".equalsIgnoreCase(fldNullableInd)) ) {
	 	 	 	 	 						if ("".equalsIgnoreCase( StringUtils.checkNull(interfaceDetailRecord.getFieldValue() ) )){
	 	 	 	 	 							interfaceDetailRecord.setErrStatus("Y");
	 	 	 	 	 							interfaceDetailRecord.setErrMessage("Validation failed"); 

	 	 	 	 	 							interfaceRecordForMap.setErrProcessMsg(StringUtils.checkNull(interfaceRecord.getErrProcessMsg()) +" Required field missing for "+packetFormDtlsResultset.getString("field_name")); 
	 	 	 	 	 							LOGGER.trace("interfaceRecordForMap Interface process4 err msg ={}",interfaceRecord.getErrProcessMsg());
	 	 	 	 	 							 
	 	 	 	 	 							
	 	 	 	 	 						    //new changes below
	 	 	 	 	 							logUtils.recordErrorSummary(packetNoInErrors+interfaceRecordForMap.getCustOrdNo().getFieldValue(), " Required field missing for "+packetFormDtlsResultset.getString("field_name"));
	 	 	 	 	 							//interfaceRecord.setErrStatus("Y"); 
	 	 	 	 	 							interfaceRecordForMap.setErrStatus(interfaceDetailRecord.getErrStatus());  
	 	 	 	 	 						
	 	  	  	 	 						}
	 	 	 	 	 					} 
	 	 	 	 	 					
	 	  	 	 						interfaceDetailList.add(interfaceDetailRecord);
	 	    	 	 				}	 	 					
	 	 	 	 					
	 		 	 				}catch(Exception e) {
	 		 	 					LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"processInterfaceRecord2 "));
	 		 	 					//logUtils.recordErrorSummary("Invalid format for record "+inInterfaceRecordStr);

	 		 	 				}finally {
	 		 	 					if ((packetFormDtlsResultset !=null) && (  !packetFormDtlsResultset.isClosed() ) ){
	 		 	 						packetFormDtlsResultset.close();
	 		 	 	    	  		} 
	 		 	 				}
	 		 	 				if (!interfaceDetailList.isEmpty()) {
	 		 	 					interfaceFormDtlsMap.put(formType, interfaceDetailList);  
	 		 	 				}
	 	  	 				}
	 	  				}catch(Exception e) {
	 	  					LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"processInterfaceRecord3 "));
	 	 				}finally { 
	 	 					if ((packetFormSeqResultset !=null) && (  !packetFormSeqResultset.isClosed() ) ){
	 	 						packetFormSeqResultset.close();
	 	 	    	  		}  
	 	 				}
	 	 				interfaceRecordForMap.setInterfaceDetailMap(interfaceFormDtlsMap);
	 	 				
	 	 				if ((interfaceRecordList !=null) && (!interfaceRecordList.isEmpty())) {
	 	 					interfaceRecordList.add(interfaceRecordForMap);
	 	 				}else {
	 	 					interfaceRecordList = new ArrayList<>();
	 	 					interfaceRecordList.add(interfaceRecordForMap);
	 	 				}
 					} 
 			 
 					inInterfaceHdrMap.put(interfaceRecord.getCustOrdNo().getFieldValue(), interfaceRecordList);		
 
 	  		}catch (Exception e) { 
 	  			LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"processInterfaceRecord0 "));
 	 		}finally{
 	    	  	try {
 	    	  		if ((packetDestTypeResultset !=null) && (  !packetDestTypeResultset.isClosed() ) ){
 	    	  			packetDestTypeResultset.close();
 	    	  		} 			
 	    	  	}catch(Exception e) {
 	    	  		LOGGER.info( ExceptionUtils.getErrorStackTrace(e,"Ignore"));
 	    	  	}
 					
 			}
 			
 			//updPorcesshdrErrRecStr
  		}  //packettype is not null
 		
 	}
	
	public void updProcessHdrErrorStatus(String inCustOrdNo,int inProcessPacketTypeSeqId,String inOrdEntrySrc,String inRecStatus,String inErrMessage, String inpacketNo) {
		String updProcesshdrErrRecStr="UPDATE printable_data_hdr \r\n "+
					                 "    SET process_ind ='B',\r\n"+
					                 "        process_status='BAD', \r\n"+
					                 "        process_msg=? \r\n"+
					                 "  WHERE stat_order_no_pst = ? \r\n"+
					                 "    AND ppmh_seq_no= ? \r\n"+
					                 "    AND order_entry_src= ? \r\n"+
					                 "    AND run_number=?"
        ; 
		 
		if (("Y".equalsIgnoreCase(inRecStatus))||("B".equalsIgnoreCase(inRecStatus))){
 			
			badRecCount++;
 			if (("B".equalsIgnoreCase(inRecStatus))) {
 				
 				try(PreparedStatement updProcesshdrErrPreparedStmt = getDbConn().prepareStatement(updProcesshdrErrRecStr);) {   
 					
 					updProcesshdrErrPreparedStmt.setString(1, inErrMessage);
 					updProcesshdrErrPreparedStmt.setString(2, inCustOrdNo);
 					updProcesshdrErrPreparedStmt.setInt(3, inProcessPacketTypeSeqId);
 					updProcesshdrErrPreparedStmt.setString(4, inOrdEntrySrc);
 					updProcesshdrErrPreparedStmt.setInt(5, getRunNumber());
 					
 	 				LOGGER.trace("updProcesshdrErrRecStr={} ",updProcesshdrErrRecStr);   
 	 				LOGGER.trace("updProcesshdrErrRecStr inErrMessage={} ",inErrMessage);
 	 				LOGGER.trace("updProcesshdrErrRecStr inCustOrdNo={} ",inCustOrdNo);
 	 				LOGGER.trace("updProcesshdrErrRecStr inProcessPacketTypeSeqId={} ",inProcessPacketTypeSeqId);
 	 				LOGGER.trace("updProcesshdrErrRecStr inOrdEntrySrc={} ",inOrdEntrySrc);
 	 				LOGGER.trace("updProcesshdrErrRecStr getRunNumber={} ",getRunNumber());
 	 				if ("Y".equalsIgnoreCase(errorFileRequired)) {
 	 					
						if ("Y".equalsIgnoreCase(multipleErrorsInoneLinePerOrder)) {
							String recIdentfierNo="rec"+getCurrentRecordNo();
							if (!"".equalsIgnoreCase(StringUtils.checkNull(inCustOrdNo)) ){
								recIdentfierNo	=inCustOrdNo;
							}else {
								if (!"".equalsIgnoreCase(StringUtils.checkNull(getCurrentStatOrderNo())) ){
									recIdentfierNo	=getCurrentStatOrderNo();
								}
							}
							
							logUtils.recordErrorSummary(recIdentfierNo,inErrMessage);
						}else {
							logUtils.recordErrorSummary(errDateStr+errorFileDelimiterValue+inpacketNo +" "+inCustOrdNo+errorFileDelimiterValue+inErrMessage);
						} 	 					
 	 					
					}else {
						logUtils.recordErrorSummary(inpacketNo +" "+inCustOrdNo+" "+inErrMessage);
					}
 					updProcesshdrErrPreparedStmt.executeUpdate();
 				}catch (Exception e) {
 					LOGGER.error(  ExceptionUtils.getErrorStackTrace(e));
 					 
 		 	 	} 					
 			}
 		}		  
	}
	
	public void processInterfaceData(Map<String,ArrayList<InterfaceRecord>> inInterfaceHdrMap, String inPacketNoInErrors) {
		 
		ResultSet packetSeqResultset = null;
 		String packetSeqSqlStr="SELECT printable_data_hdr_seq.nextval pfdh_seq \r\n"+
 			             	   "  FROM DUAL   "
  		;
 		//ppmh_seq_no
 		int insPacketHdrStatus;
 		int insPacketDtlStatus; 
 		String packetNoInErrors=null;
  		String insPacketDataSqlStr="INSERT INTO printable_data_hdr (pfdh_seq,\r\n"+
					                "                                ppmh_seq_no,\r\n" +
					                "                                order_entry_src,\r\n" + 
					                "                                edi_tp_id_pst,\r\n" + 
					                "                                emp_package_type,\r\n" + 
					                "                                stat_order_no_pst,\r\n" + 
					                "                                stat_ord_line_no,\r\n" + 
					                "                                dtl_line_no,  \r\n" + 
					                "                                item_no,       \r\n" + 
					                "                                order_date,         \r\n" + 
					                "                                qty,  \r\n" + 
					                "                                uom_code,          \r\n" + 
					                "                                unit_cost,\r\n" + 
					                "                                emp_no,\r\n" + 
					                "                                emp_name,\r\n" + 
					                "                                emp_first_name,\r\n" + 
					                "                                emp_last_name,\r\n" + 
					                "                                emp_middle_name, \r\n" + 
					                "                                ship_attn,\r\n" + 
					                "                                ship_addr1,           \r\n" + 
					                "                                ship_addr2,          \r\n" + 
					                "                                ship_addr3,           \r\n" + 
					                "                                ship_city,            \r\n" + 
					                "                                ship_state,           \r\n" + 
					                "                                ship_zip_code,        \r\n" + 
					                "                                billto_cd,\r\n" + 
					                "                                shipto_cd,\r\n" + 
					                "                                cust_no,             \r\n" + 
					                "                                finance_no,  \r\n" + 
					                "                                priority_code,   \r\n" + 
					                "                                service_type,\r\n" + 
					                "                                replace_ind,\r\n" + 
					                "                                brkn_serial_number, \r\n" + 
					                "                                alter_ship_attn,\r\n" + 
					                "                                alter_ship_addr1,\r\n" + 
					                "                                alter_ship_addr2,\r\n" + 
					                "                                alter_ship_addr3,\r\n" + 
					                "                                alter_ship_city,\r\n" + 
					                "                                alter_ship_state,\r\n" + 
					                "                                alter_ship_zip_code,\r\n" + 
					                "                                destination_type,  \r\n" + 
					                "                                packet_no,\r\n" + 
					                "                                run_number,\r\n"+
					                "                                process_ind,\r\n" + 
					                "                                process_status, \r\n"+
					                "                                ship_country, \r\n"+
					                "                                alt_ship_country, \r\n"+
					                "                                process_msg) \r\n"+
					                "                        VALUES (?,\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(replace(?,'-')),\r\n" + 
					                "                                to_date(trim(?),'YYYYMMDD'),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                replace(trim(?),'-'),\r\n" + 
					                "                                trim(?) ,   \r\n" + 
					                "                                trim(?) ,   \r\n" + 
					                "                                trim(?) ,   \r\n" + 
					                "                                trim(?),         \r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                replace(trim(?),'-'),\r\n" + 
					                "                                trim(?),\r\n" + 
					                "                                ?,\r\n" + 
					                "                                ?, \r\n "+  
					                "                                ? ,\r\n" +
					                "                                ? ,\r\n" + 
					                "                                NVL(?, 'US'),\r\n" + 
					                "                                NVL(?, 'US') ,\r\n" + 
					                "                                SUBSTR(?, 1, 4000)) "  
					                ;
 		 
  		String insPacketDtlDataSqlStr="INSERT INTO printable_data_dtl (pfdh_seq,\r\n "+
						                "                                pfmd_seq_no, \r\n"+
						                "                                pfmh_seq_no, \r\n"+
						                "                                field_name, \r\n"+
						                "                                field_value,\r\n"+
						                "                                outbound_header_name ,\r\n"+
						                "                                outbound_seq, \r\n"+ 
						                "                                ack_outbound_seq \r\n"+ 
						                "                               ) \r\n"+
						                "                        VALUES(?,\r\n"+
						                "                               ? ,\r\n"+
						                "                               ?,\r\n"+
						                "                               ?, \r\n"+
						                "                               mdims_data_util.f_get_wrapped_map_data('NPC_FILE_CONVERSION', 'F',?,get_formated_value(trim(?), ?,trim(?))  ) ,\r\n"+
						                "                               ?,\r\n"+
						                "                               ?,\r\n"+
						                "                               ? \r\n"+
						                "                             )" ;    		

  		LOGGER.info("In processInterfaceData"); 
			 	
  		for (Map.Entry<String,ArrayList<InterfaceRecord>> interfaceHdrMapRec : inInterfaceHdrMap.entrySet()) {
 			String interfaceHdrMapKey =interfaceHdrMapRec.getKey();
 			ArrayList<InterfaceRecord> interfaceHdrList =interfaceHdrMapRec.getValue();
 			String insertErrMessage="";
 			int recNumber =1;
 			int recInsertStatus=0;
 			int packetSeqLocal=0;
 			String ordEntrySrcLocal=""; 
 			if ((interfaceHdrList !=null) && (!interfaceHdrList.isEmpty())){
	 			for (InterfaceRecord interfaceHdrRec:interfaceHdrList ) {
	 				String errStatusLocal="NEW";
	 				if (!"N".equalsIgnoreCase(interfaceHdrRec.getErrStatus())) {
	 					errStatusLocal="BAD";
	 				}
	  				//if "Y".equalsIgnoreCase(interfaceHdrRec.getErrStatus 
   					int pfdhSeq=-1;
  					insPacketHdrStatus=0;
  					packetSeqLocal=interfaceHdrRec.getPacketSeq();
  					ordEntrySrcLocal=interfaceHdrRec.getOrdEntrySrc().getFieldValue();
 			       	
  					try(PreparedStatement packetSeqPreparedStmt = getDbConn().prepareStatement(packetSeqSqlStr);) {   
 					  	
 					  	packetSeqResultset=packetSeqPreparedStmt.executeQuery();
 					  	while (packetSeqResultset.next()) {
         					pfdhSeq=packetSeqResultset.getInt("pfdh_seq");
         					break;
        				}
        				if (pfdhSeq>=0){
        					LOGGER.trace("In pfdhSeq={}", pfdhSeq);
        					LOGGER.trace("CustOrdNo ={}", interfaceHdrRec.getCustOrdNo().getFieldValue());
        					LOGGER.trace("CustOrdLineNo={}", interfaceHdrRec.getCustOrdLineNo().getFieldValue());
         			       	
        					try(PreparedStatement insPacketDataPreparedStmt = getDbConn().prepareStatement(insPacketDataSqlStr);) {  
        			       		
        						if ("Y".equalsIgnoreCase(inPacketNoInErrors)) {
        				 			packetNoInErrors=interfaceHdrRec.getPacketType().getFieldValue();
        				 		}
        						
        			       		insPacketDataPreparedStmt.setInt(1, pfdhSeq);
        			       		insPacketDataPreparedStmt.setInt(2, interfaceHdrRec.getPacketSeq());
        			       		insPacketDataPreparedStmt.setString(3, StringUtils.checkNull(interfaceHdrRec.getOrdEntrySrc().getFieldValue()));
        			       		insPacketDataPreparedStmt.setString(4, StringUtils.checkNull(interfaceHdrRec.getEdiTpId().getFieldValue()));
        			       		insPacketDataPreparedStmt.setString(5, StringUtils.checkNull(interfaceHdrRec.getPacketType().getFieldValue()));
        			       		insPacketDataPreparedStmt.setString(6, StringUtils.checkNull(interfaceHdrRec.getCustOrdNo().getFieldValue()));
        			       		insPacketDataPreparedStmt.setString(7, StringUtils.checkNull(interfaceHdrRec.getCustOrdLineNo().getFieldValue()));
        			       		insPacketDataPreparedStmt.setInt(8, recNumber);
        			       		insPacketDataPreparedStmt.setString(9, StringUtils.checkNull(interfaceHdrRec.getItemNo().getFieldValue()));
        			       		insPacketDataPreparedStmt.setString(10, StringUtils.checkNull(interfaceHdrRec.getOrderDate().getFieldValue()));
        			       		insPacketDataPreparedStmt.setString(11, StringUtils.checkNull(interfaceHdrRec.getOrderQty().getFieldValue()));
        			       		insPacketDataPreparedStmt.setString(12, StringUtils.checkNull(interfaceHdrRec.getUnitOfMeasure().getFieldValue()));
        			       		insPacketDataPreparedStmt.setString(13, StringUtils.checkNull(interfaceHdrRec.getUnitCost().getFieldValue()));
        			       		insPacketDataPreparedStmt.setString(14, StringUtils.checkNull(interfaceHdrRec.getEmpId().getFieldValue()));
           			       		insPacketDataPreparedStmt.setString(15, StringUtils.checkNull(interfaceHdrRec.getEmpName().getFieldValue()));
           			       		insPacketDataPreparedStmt.setString(16, StringUtils.checkNull(interfaceHdrRec.getCustFirstName().getFieldValue()));
           			       		insPacketDataPreparedStmt.setString(17, StringUtils.checkNull(interfaceHdrRec.getCustLastName().getFieldValue()));
           			       		insPacketDataPreparedStmt.setString(18, StringUtils.checkNull(interfaceHdrRec.getCustMiddleName().getFieldValue()));
           			       		insPacketDataPreparedStmt.setString(19, StringUtils.checkNull(interfaceHdrRec.getCustAttention().getFieldValue()));
           			       		insPacketDataPreparedStmt.setString(20, StringUtils.checkNull(interfaceHdrRec.getCustAddress1().getFieldValue()));
           			       		insPacketDataPreparedStmt.setString(21, StringUtils.checkNull(interfaceHdrRec.getCustAddress2().getFieldValue()));
           			       		insPacketDataPreparedStmt.setString(22, StringUtils.checkNull(interfaceHdrRec.getCustAddress3().getFieldValue()));
           			       		insPacketDataPreparedStmt.setString(23, StringUtils.checkNull(interfaceHdrRec.getCustCity().getFieldValue()));
           			       		insPacketDataPreparedStmt.setString(24, StringUtils.checkNull(interfaceHdrRec.getCustState().getFieldValue())); 
               			       	insPacketDataPreparedStmt.setString(25, StringUtils.checkNull(interfaceHdrRec.getCustZipCode().getFieldValue())); 
           			       		
           			       		if ( !"".equalsIgnoreCase(StringUtils.checkNull(interfaceHdrRec.getBillToFedstrip().getFieldValue()))) {
           			       			insPacketDataPreparedStmt.setString(26, interfaceHdrRec.getBillToFedstrip().getFieldValue());
           			       		}else {
           			       			if ( !"".equalsIgnoreCase(StringUtils.checkNull(interfaceHdrRec.getCustNo().getFieldValue()))) {
                			       		insPacketDataPreparedStmt.setString(26, interfaceHdrRec.getCustNo().getFieldValue());
               			       		}else {
               			       			insPacketDataPreparedStmt.setString(26, StringUtils.checkNull(interfaceHdrRec.getShipToCustNo().getFieldValue()));
                			       	}
           			       		}
           			       		if ( !"".equalsIgnoreCase(StringUtils.checkNull(interfaceHdrRec.getShipToCustNo().getFieldValue()))) {
           			       			insPacketDataPreparedStmt.setString(27, interfaceHdrRec.getShipToCustNo().getFieldValue());
           			       		}else {
           			       			if ( !"".equalsIgnoreCase(StringUtils.checkNull(interfaceHdrRec.getCustNo().getFieldValue()))) {
            			       			insPacketDataPreparedStmt.setString(27, interfaceHdrRec.getCustNo().getFieldValue());
           			       			}else {
           			       				insPacketDataPreparedStmt.setString(27, StringUtils.checkNull(interfaceHdrRec.getBillToFedstrip().getFieldValue()));
           			       			}
           			       		}
           			       		
           			    		if ( !"".equalsIgnoreCase(StringUtils.checkNull(interfaceHdrRec.getCustNo().getFieldValue()))) {
           			       			insPacketDataPreparedStmt.setString(28, interfaceHdrRec.getCustNo().getFieldValue());
           			       		}else {
           			       			if ( !"".equalsIgnoreCase(StringUtils.checkNull(interfaceHdrRec.getBillToFedstrip().getFieldValue()))) {
            			       			insPacketDataPreparedStmt.setString(28, interfaceHdrRec.getBillToFedstrip().getFieldValue());
           			       			}else {
           			       				insPacketDataPreparedStmt.setString(28, StringUtils.checkNull(interfaceHdrRec.getShipToCustNo().getFieldValue()));
           			       			}
           			       		}    		
   			       				insPacketDataPreparedStmt.setString(29, StringUtils.checkNull(interfaceHdrRec.getCustFinanceNo().getFieldValue()));
   			       				insPacketDataPreparedStmt.setString(30, StringUtils.checkNull(interfaceHdrRec.getPriorityCode().getFieldValue()));
            			       		
  			       				insPacketDataPreparedStmt.setString(31, StringUtils.checkNull(interfaceHdrRec.getServiceTypeCode().getFieldValue()));
   			       				insPacketDataPreparedStmt.setString(32, StringUtils.checkNull(interfaceHdrRec.getReplaceInd().getFieldValue()));
  			       				insPacketDataPreparedStmt.setString(33, StringUtils.checkNull(interfaceHdrRec.getBrokenPartSerialNo().getFieldValue()));
   			       				insPacketDataPreparedStmt.setString(34, StringUtils.checkNull(interfaceHdrRec.getAltCustAttention().getFieldValue()));
   			       				insPacketDataPreparedStmt.setString(35, StringUtils.checkNull(interfaceHdrRec.getAltCustAddress1().getFieldValue()));
   			       				insPacketDataPreparedStmt.setString(36, StringUtils.checkNull(interfaceHdrRec.getAltCustAddress2().getFieldValue()));
   			       				insPacketDataPreparedStmt.setString(37, StringUtils.checkNull(interfaceHdrRec.getAltCustAddress3().getFieldValue()));

   			       				insPacketDataPreparedStmt.setString(38, StringUtils.checkNull(interfaceHdrRec.getAltCustCity().getFieldValue()));
   			       				insPacketDataPreparedStmt.setString(39, StringUtils.checkNull(interfaceHdrRec.getAltCustState().getFieldValue()));
   			       				insPacketDataPreparedStmt.setString(40, StringUtils.checkNull(interfaceHdrRec.getAltCustZipCode().getFieldValue()));
   			       				insPacketDataPreparedStmt.setString(41, StringUtils.checkNull(interfaceHdrRec.getDestinationType()));
   			       				insPacketDataPreparedStmt.setString(42, StringUtils.checkNull(interfaceHdrRec.getPacketNo()));
   			       				insPacketDataPreparedStmt.setInt(43, getRunNumber());
   			       				insPacketDataPreparedStmt.setString(44, StringUtils.checkNull(interfaceHdrRec.getErrStatus(),"N"));
   			       				insPacketDataPreparedStmt.setString(45, errStatusLocal);
   			       				insPacketDataPreparedStmt.setString(46, StringUtils.checkNull(interfaceHdrRec.getCustCountry().getFieldValue()));
   			       			    insPacketDataPreparedStmt.setString(47, StringUtils.checkNull(interfaceHdrRec.getAltCustCountry().getFieldValue()));
   			       			    insPacketDataPreparedStmt.setString(48, StringUtils.checkNull(interfaceHdrRec.getErrProcessMsg()));
           					  	insPacketHdrStatus  =insPacketDataPreparedStmt.executeUpdate();
           					  	LOGGER.trace("header insert insPacketHdrStatus ={}", insPacketHdrStatus);
           					  	
         					  	if (insPacketHdrStatus >0) {

         					  		//interfaceFormDtlsMap	 interfaceHdrList.get
         	    		       		 
         					  		HashMap<String,ArrayList<InterfaceDetailRecord>>interfaceFormDtlsMap= (HashMap<String, ArrayList<InterfaceDetailRecord>>) interfaceHdrRec.getInterfaceDetailMap();
         					  		
          					   		for (Map.Entry<String,ArrayList<InterfaceDetailRecord>> dtlMap : interfaceFormDtlsMap.entrySet()) {
         					  			ArrayList<InterfaceDetailRecord> dtlList=dtlMap.getValue();
         					  			for (InterfaceDetailRecord dtlRecord:dtlList){
         					  				try(PreparedStatement insPacketDtlDataPreparedStmt = getDbConn().prepareStatement(insPacketDtlDataSqlStr);) {  
	             					  			 
	             					  			insPacketDtlDataPreparedStmt.setInt(1, pfdhSeq);
	             					  			insPacketDtlDataPreparedStmt.setInt(2, dtlRecord.getFieldSeqNo());
	             					  			insPacketDtlDataPreparedStmt.setInt(3, dtlRecord.getFormSeqNo());
	             					  			insPacketDtlDataPreparedStmt.setString(4, dtlRecord.getFieldName());
	             					  			
	             					  			insPacketDtlDataPreparedStmt.setInt(5, dtlRecord.getFieldSeqNo());  // new variable added 
	             					  			
	             					  			insPacketDtlDataPreparedStmt.setString(6, StringUtils.checkNull(dtlRecord.getFieldValue()));
	             					  			insPacketDtlDataPreparedStmt.setString(7, StringUtils.checkNull(dtlRecord.getFieldFormatType()));
	             					  			insPacketDtlDataPreparedStmt.setString(8,  interfaceHdrRec.getApplicationName());
	             					  			
	             					  			insPacketDtlDataPreparedStmt.setString(9,  StringUtils.checkNull(dtlRecord.getObHeaderName()));
	             					  			insPacketDtlDataPreparedStmt.setInt(10, dtlRecord.getOutboundSeq());
	             					  			insPacketDtlDataPreparedStmt.setInt(11, dtlRecord.getAckOutboundSeq());
	             					  			insPacketDtlStatus=insPacketDtlDataPreparedStmt.executeUpdate();
	             					  			if (insPacketDtlStatus<=0) {
	             					  				recInsertStatus=0;
	             					  			}else {
	             					  				recInsertStatus=insPacketDtlStatus;
	             					  			}
	             					  			LOGGER.trace("detail insert recInsertStatus ={}", recInsertStatus);
	                			       		}catch(Exception e) {
	                			       			LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"Insert printable_data_dtl Error "));
	                 			       			recInsertStatus=0;
	                 			       			insertErrMessage=ExceptionUtils.getErrorStackTrace(e,"Insert printable_data_dtl Error ");
	                 						} 
         					  			}
         					  			if ("PACT".equalsIgnoreCase(interfaceHdrRec.getOrdEntrySrc().getFieldValue())) {
         					  				if ("PACT_P1".equalsIgnoreCase(interfaceHdrRec.getPacketType().getFieldValue())) {
         					  				  //call database procedure
         					  			        try(CallableStatement cs = getDbConn().prepareCall("{call gen_print_forms_orders_pkg.pact_data_load_by_refrence_type(?, ?,?,?)}");) { 
         					  			 			LOGGER.info("pact_data_load_by_refrence_type- start: {} ",pfdhSeq);
         					  			 			cs.setInt(1, pfdhSeq);
         					  			 			cs.setString(2, "PACT_P1");
         					  			  			cs.registerOutParameter(3, Types.VARCHAR);  
         					  			  			cs.registerOutParameter(4, Types.VARCHAR);  
         					  						cs.execute();
         					  			        }catch(Exception e) {
         					  			        	LOGGER.error("pact_data_load_by_refrence_type- error: {}",e.getMessage());
         					  			        }
         					  				}
         					  			}
         					  		} 
 	        			       	} else{
 	        			       		recInsertStatus=0;
 	        			       	}
         			       	}catch(SQLException e) {
         			       		LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"Insert printable_data_hdr Error "));
         			       		recInsertStatus=0;
         			       		insertErrMessage=e.getMessage();
         					}catch(Exception e) {
         			       		LOGGER.error(  "Insert printable_data_hdr Error={}", e.getMessage());
         			       		recInsertStatus=0;
         			       		insertErrMessage=e.getMessage();
         					} 	
  						} // if pfdhSeq >0
 			       	}catch(Exception e) {
 			       		LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"Error process data "));
 			       		 
 					}finally {
				  		try {
				  			 if ((packetSeqResultset !=null) && (  !packetSeqResultset.isClosed() ) ){
								 packetSeqResultset.close();
							 } 
				  		}catch(Exception e) {
				  			LOGGER.info(  ExceptionUtils.getErrorStackTrace(e,"Ignore "));
				  		}
				  	}
 			        
 			       	recNumber++;
	 			}
 	 			if (recInsertStatus<=0) {
	 				//interfaceHdrMapKey
	 				updProcessHdrErrorStatus(interfaceHdrMapKey,packetSeqLocal,ordEntrySrcLocal,"B",insertErrMessage, packetNoInErrors);  
	 			}
	 			
 			}
 		}
	}
	 
	public String processIntRecords(  String inProcess, String inEntrySrc, int inRunNbr, String inValidateDupRecords, String inPacketNoInErrors ){
		 
		String outReturnMsg = "";  
		ResultSet rset1 = null;
		String selectStmt1 = " SELECT SUM(decode(hdr.process_status, 'ERROR',1,'BAD', 1, 0)) bad_cnt, \n " 
								 + " SUM(decode(hdr.process_status, 'ERROR', 0,'BAD', 0, 1)) good_cnt \n " 
							+ " FROM printable_data_hdr hdr \n "
						   + " WHERE hdr.run_number = ? \n "
						     + " AND hdr.order_entry_src = ? \n"; 
		String selectStmt2 = " SELECT stat_order_no_pst,packet_no,process_msg \n " 
				 			 + " FROM printable_data_hdr hdr \n "
				 			+ " WHERE hdr.run_number = ? \n "
				 			  + " AND hdr.process_ind = 'E' \n"; 

		// Connection inConn = null 
		ResultSet printErrorResultset = null;

        try(CallableStatement cs = getDbConn().prepareCall("{call gen_print_forms_orders_pkg.validate_create_order(?, ?, ?, ?, ?, ?)}");) { 
			// inConn = getConnection1()
 			LOGGER.info("processIntRecords- start");
			 
			cs.setString(1, inProcess);
			cs.setString(2, inEntrySrc); 
			cs.setInt(3, inRunNbr); 
			cs.setInt(4, getDbLogLevel()); 
			cs.setString(5, inValidateDupRecords);
			//cs.setString(6, inIgnoreZipAddress)
  			cs.registerOutParameter(6, Types.VARCHAR);  
			
			cs.execute();
			if (cs.getString(6) !=null)	{
				outReturnMsg=cs.getString(6);
			} 
			 
			try(PreparedStatement stmt1 = getDbConn().prepareStatement(selectStmt1);) {  
				
				stmt1.setInt(1, inRunNbr); 
				stmt1.setString(2, inEntrySrc);
				rset1 = stmt1.executeQuery(); 
				while (rset1.next()){ 
					//setBadRecCount(rset1.getInt("bad_cnt")+getBadRecCount
					setBadRecCount(rset1.getInt("bad_cnt"));
					setGoodRecCount(rset1.getInt("good_cnt")); 
				}
			} catch(Exception e) {
	       		LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"Error stmt1 ")); 
			}finally {
		  		try {
		  			 if ((rset1 !=null) && (  !rset1.isClosed() ) ){
		  				rset1.close();
					 } 
		  		}catch(Exception e) {
		  			LOGGER.info(  ExceptionUtils.getErrorStackTrace(e,"Ignore "));
		  		}
		  	}
 			 
			LOGGER.info("bad_cnt, good_cnt for record : {} , {}",getBadRecCount() , getGoodRecCount());
			
			try(PreparedStatement printErrorPreparedStmt = getDbConn().prepareStatement(selectStmt2);) {   
			 
				printErrorPreparedStmt.setInt(1, inRunNbr); 
				printErrorResultset=printErrorPreparedStmt.executeQuery();
				while (printErrorResultset.next()) {

					String recPacketNo = "";
					if("Y".equalsIgnoreCase(inPacketNoInErrors)) {
						recPacketNo     =printErrorResultset.getString("packet_no")+errorFileDelimiterValue;
					}
					
					if ("Y".equalsIgnoreCase(errorFileRequired)) {
						if ("Y".equalsIgnoreCase(multipleErrorsInoneLinePerOrder)) {
							
							String recIdentfierNo="rec"+getCurrentRecordNo();
							if (!"".equalsIgnoreCase(StringUtils.checkNull(printErrorResultset.getString("stat_order_no_pst"))) ){
								recIdentfierNo	=printErrorResultset.getString("stat_order_no_pst"); 
							} 
							
							logUtils.recordErrorSummary(recPacketNo+recIdentfierNo,printErrorResultset.getString("process_msg"));
						}else {
							logUtils.recordErrorSummary(errDateStr+errorFileDelimiterValue+recPacketNo+printErrorResultset.getString("stat_order_no_pst")+errorFileDelimiterValue+printErrorResultset.getString("process_msg"));
						}	 				
					}else {
						logUtils.recordErrorSummary(recPacketNo+printErrorResultset.getString("stat_order_no_pst")+" "+printErrorResultset.getString("process_msg"));
					}
				}
			} catch(Exception e) {
	       		LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"Error selectStmt2 ")); 
			}finally {
		  		try {
		  			 if ((printErrorResultset !=null) && (  !printErrorResultset.isClosed() ) ){
		  				printErrorResultset.close();
					 } 
		  		}catch(Exception e) {
		  			LOGGER.info(  ExceptionUtils.getErrorStackTrace(e,"Ignore "));
		  		}
		  	}
			 
  		} catch (Exception e){ 
 			LOGGER.error(  "Exception processIntRecords-{} ",e.getMessage()); 
 			 
		} finally{
       	 	try {
       		 
        		if ((rset1 !=null) && (!rset1.isClosed() ) ) {
        			rset1.close();
        		}  
            }catch(Exception e) {
            	LOGGER.error(ExceptionUtils.getErrorStackTrace(e),"ignorable Error "); 
            } 
        }
		return outReturnMsg;
	} 
 
	
	public void setPacketData(InterfaceRecord inInterfaceRecord, String inPacketKey,FieldAttributes inFldData) {

		if ("TS_ID_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setEdiTpId(inFldData);	
		}
		if ("ORD_ENTRY_SRC_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setOrdEntrySrc(inFldData);	
		}
			
		if ("PACKGETYPE_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setPacketType(inFldData);	
		}
		
		if ("CUSTOMER_ORDER_NUM_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustOrdNo(inFldData);	
		}
 		
		if ("CUST_ORD_LINE_NO_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustOrdLineNo(inFldData);	
		}

		if ("ITEM_NO_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setItemNo(inFldData);	
		}
		if ("ORDER_DATE_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setOrderDate(inFldData);	
		}

		if ("ORDER_QUANTITY_KEY".equalsIgnoreCase(inPacketKey)){
			if (inFldData.getFieldValue().trim() !=null) {
				LOGGER.info(  "inFldData.getFieldValue()={} ",inFldData.getFieldValue());
				//LOGGER.info(  "inFldData.getFieldValue() trim={} ",inFldData.getFieldValue().trim()); 
				
				inFldData.setFieldValueInt(Integer.parseInt(inFldData.getFieldValue().trim()));
				 
			}else {
				inFldData.setFieldValueInt(1);
			}
			inInterfaceRecord.setOrderQty(inFldData);	
			
		}
		if ("UNIT_OF_MEASURE_KEY".equalsIgnoreCase(inPacketKey)){ 
			inInterfaceRecord.setUnitOfMeasure(inFldData);	
		}
		
		if ("CUST_ATTN_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustAttention(inFldData);	
		}

		if ("CUSTOMER_ADDRESS_1_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustAddress1(inFldData);	
		}
		if ("CUSTOMER_ADDRESS_2_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustAddress2(inFldData);	
		}
		if ("CUSTOMER_ADDRESS_3_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustAddress3(inFldData);	
		}
		if ("CUSTOMER_CITY_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustCity(inFldData);	
		}
		if ("CUSTOMER_STATE_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustState(inFldData);	
		}
		
		if ("CUSTOMER_ZIP_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustZipCode(inFldData);	
		}
		if ("UNIT_COST_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setUnitCost(inFldData);	
		}
		if ("TOTAL_COST_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setTotalCost(inFldData);	
		}
		//1
		if ("CUST_NO_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustNo(inFldData);	
		}
		if ("BILL_TO_FEDSTRIP_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setBillToFedstrip(inFldData);	
		}
		if ("FINANCE_NO_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustFinanceNo(inFldData);	
		}
		if ("PRIORITY_CD_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setPriorityCode(inFldData);	
		}
		if ("SERVICE_TYPE_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setServiceTypeCode(inFldData);	
		}

		if ("REPLACE_IND_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setReplaceInd(inFldData);	
		}
		if ("BRKN_SERIAL_NUMBER_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setBrokenPartSerialNo(inFldData);	
		}
		if ("EMP_ID_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setEmpId(inFldData);	
		}

		if ("EMP_NAME_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setEmpName(inFldData);	
		}
		
		if ("CUST_FIRST_NAME_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustFirstName(inFldData);	
		}
		//2
		if ("CUST_LAST_NAME_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustLastName(inFldData);	
		}
		//3
		if ("CUST_MIDDLE_NAME_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustMiddleName(inFldData);	
		}
		 
		//4
		if ("ALT_CUST_ATTN_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setAltCustAttention(inFldData);	
		}
		
		if ("ALT_CUST_ADDR1_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setAltCustAddress1(inFldData);	
		}
		if ("ALT_CUST_ADDR2_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setAltCustAddress2(inFldData);	
		}
		if ("ALT_CUST_ADDR3_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setAltCustAddress3(inFldData);	
		}
		if ("ALT_CUST_CITY_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setAltCustCity(inFldData);	
		}
		if ("ALT_CUST_STATE_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setAltCustState(inFldData);	
		}
		if ("ALT_ZIP_CODE_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setAltCustZipCode(inFldData);	
		}
		
		//Added country for PACT as address validation applies to USA only
		
		if ("CUSTOMER_COUNTRY_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setCustCountry(inFldData);	
		}

		if ("ALT_CUST_COUNTRY_KEY".equalsIgnoreCase(inPacketKey)){
			inInterfaceRecord.setAltCustCountry(inFldData);	
		}
		 
	}
	
	public void setProcessErrorInfo(InterfaceRecord inInterfaceRecord,String errMessage) {
		ProcessErrorhdr processErrorhdr ;
		processErrorhdr=inInterfaceRecord.getProcessErrorhdr();
		if (processErrorhdr ==null) {
			processErrorhdr= new ProcessErrorhdr();
			processErrorhdr.setProcessId(getInterfaceName());
 			processErrorhdr.setRunNbr(getRunNumber());
			processErrorhdr.setInterfaceKey(inInterfaceRecord.getCustOrdNo().getFieldValue());
 		}
 		ArrayList< ProcessErrorDtl> processErrorDtlList = processErrorhdr.getProcessErrorDtlList();
 		if (processErrorDtlList !=null) {
			ProcessErrorDtl processErrorDtl= new ProcessErrorDtl();
			processErrorDtl.setCustomerErrorNotes(errMessage);
			processErrorDtl.setInterfaceErrorMessage(errMessage);
			processErrorDtlList.add(processErrorDtl);
			processErrorhdr.setProcessErrorDtlList(processErrorDtlList);
 			
 		}else {
 			processErrorDtlList = new ArrayList<>();
			ProcessErrorDtl processErrorDtl= new ProcessErrorDtl();
			processErrorDtl.setCustomerErrorNotes(errMessage);
			processErrorDtl.setInterfaceErrorMessage(errMessage);
			processErrorDtlList.add(processErrorDtl);
			processErrorhdr.setProcessErrorDtlList(processErrorDtlList);
 		}
 		inInterfaceRecord.setProcessErrorhdr(processErrorhdr);
 
	}
	public void setErrorInfo(InterfaceRecord inInterfaceRecord,String inErrStatus) { 
		
		ErrorHdrRecord errorhdr ;
		errorhdr=inInterfaceRecord.getErrRecord();

		inInterfaceRecord.setValidationSuccess(false);
		inInterfaceRecord.setErrStatus(inErrStatus);
  		if (errorhdr ==null) {
			errorhdr= new ErrorHdrRecord();
			errorhdr.setProcessId(getInterfaceName());
			errorhdr.setRunNbr(getRunNumber());
			errorhdr.setInterfaceKey(inInterfaceRecord.getCustOrdNo().getFieldValue());
 		}
  
		ArrayList< ErrorDetailsRecord> errDtlsInitList = (ArrayList< ErrorDetailsRecord>)errorhdr.getErrDetailsList() ;

		ErrorDetailsRecord errDtlRecord = new ErrorDetailsRecord();
		errDtlRecord.setSystemErrorMessage(inErrStatus);  
		errDtlsInitList.add(errDtlRecord);
		errorhdr.setErrDetailsList(errDtlsInitList);
		inInterfaceRecord.setErrRecord(errorhdr);

	}
	
	public boolean getFldValueValidation(FieldAttributes inFldData) {
		boolean fieldValueValidated= true;
		if ("Date".equalsIgnoreCase(inFldData.getFieldType())) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(inFldData.getFieldFormatType());
				@SuppressWarnings("unused")
				Date date1 = sdf.parse(inFldData.getFieldValue());
 			}catch(Exception e) {
 				LOGGER.error(ExceptionUtils.getErrorStackTrace(e,"Error ")) ;
 				fieldValueValidated =false;
			}
		}
		return fieldValueValidated;
	}
	
	public String validateAddress( Connection inConn, String inEntrySrc, int inRunNbr){
		   
	      String outReturnMsg	 = "";
	      ResultSet rset = null;
	      //+ "  	XMLElement(\"Address3\",ship_addr3 )||                    					\n " 
	  	  String selectStmt  =    " SELECT order_entry_src                                                           ,        \n " 
			                    + "        process_status                                                              ,        \n "  
			                    + "        decode(destination_type, 'W', alter_ship_addr1, ship_addr1 ) ship_addr1     ,        \n "
			                    + "        decode(destination_type, 'W', alter_ship_addr2, ship_addr2 ) ship_addr2     ,        \n "
			                    + "        decode(destination_type, 'W', alter_ship_addr3, ship_addr3 ) ship_addr3     ,        \n "
			                    + "        decode(destination_type, 'W', alter_ship_attn, ship_attn )   ship_attn      ,        \n "
			                    + "        decode(destination_type, 'W', alter_ship_city, ship_city )   ship_city      ,        \n "
			                    + "        decode(destination_type, 'W', alter_ship_state, ship_state ) ship_state     ,        \n "
			                    + "        decode(destination_type, 'W', alter_ship_zip_code, ship_zip_code ) ship_zip_code ,   \n "    
			                    + "        substr(replace(decode(destination_type, 'W', alter_ship_zip_code, ship_zip_code ),'-'),1,5) ship_zip5 ,   \n "    
			                    + "        substr(replace(decode(destination_type, 'W', alter_ship_zip_code, ship_zip_code ),'-'), 6) ship_zip4 ,   \n "
			                    + "        decode(destination_type, 'W', NVL(alt_ship_country,'US'), NVL(ship_country, 'US')) ship_country,\n "
			                    + "        pfdh_seq                     ,                                                       \n "                                
			                    + "        ( XMLElement(\"FirmName\" )||                                                        \n "  
			                    + "        XMLElement(\"Address1\",decode(destination_type, 'W', alter_ship_addr1, ship_addr1 ) )||                                          \n "
			                    + "        XMLElement(\"Address2\",decode(destination_type, 'W', alter_ship_addr2, ship_addr2 ) )||                                          \n "
			                    + "        XMLElement(\"City\",decode(destination_type, 'W', alter_ship_city, ship_city ) )||                                                \n "
			                    + "        XMLElement(\"State\",decode(destination_type, 'W', alter_ship_state, ship_state ) )||                                             \n "
			                    + "        XMLElement(\"Zip5\",substr(replace (decode(destination_type, 'W', alter_ship_zip_code, ship_zip_code ), '-'), 1, 5) )||           \n "                           
			                    + "        XMLElement(\"Zip4\",substr(replace (decode(destination_type, 'W', alter_ship_zip_code, ship_zip_code ), '-'), 6) )) AddressXml    \n " 
			                    + "   FROM printable_data_hdr                                                                    \n " 
			                    + "  WHERE run_number = ?                                                                        \n "
			                    + "    AND order_entry_src  =   ?                                                                \n "
			                    + "    AND process_status     IN ('NEW', 'ERROR')                                                  \n ";
	  	  
	  	  //Fixed alter_ship_country to alt_ship_country
	  	  //ADDED substr(decode(destination_type, 'W', alter_ship_zip_code, ship_zip_code ),1,5) ship_zip5 ,   \n "    
	  	  //Fixed alter_ship_country to alt_ship_country

	  	  
	  	 String updateErrorStmt  =    "  UPDATE printable_data_hdr   \n " 
					        		+ "     SET process_status    = 'ERROR'    ,  \n " 
					        		+ "         process_ind    = 'E'    ,  \n " 
					        		+ "         process_msg  = SUBSTR(DECODE(process_msg, 'ERROR', process_msg|| ' - '||?, ? ) , 1, 4000)  \n " 
					        		+ "   WHERE pfdh_seq = ?    \n";
		  	 
	  	 String updateDlvryPtStmt  =   " UPDATE printable_data_hdr   \n "+ 
	  			 						"   SET  ship_dlvry_point   = decode(destination_type, 'W',ship_dlvry_point, ? ),\r\n" + 
	  			 						"        alter_ship_dlvry_point   = decode(destination_type, 'W',?,alter_ship_dlvry_point ),\r\n" + 
	  			 						"        ship_city = decode(destination_type, 'W',ship_city, ? ),\r\n" + 
	  			 						"        alter_ship_city = decode(destination_type, 'W', ?, alter_ship_city),\r\n" + 
	  			 						"        ship_state = decode(destination_type, 'W',ship_state, ? ),\r\n" + 
	  			 						"        alter_ship_state = decode(destination_type, 'W', ?,alter_ship_state ),\r\n" + 
	  			 						"        ship_zip_code = decode(destination_type, 'W',ship_zip_code, ? ),\r\n" + 
	  			 						"        alter_ship_zip_code = decode(destination_type, 'W', ? ,alter_ship_zip_code ) "+
					                   "  WHERE pfdh_seq = ?  \n";    

	  	 /*changes decode in update statement*/
	  	 /*
	  	  					                 + "    SET decode(destination_type, 'W', alter_ship_dlvry_point, ship_dlvry_point ) = ?   ,  \n "    
					                 + "        decode(destination_type, 'W', alter_ship_city, ship_city )               = ?   ,  \n "
					                 + "        decode(destination_type, 'W', alter_ship_state, ship_state )             = ?   ,  \n "
					                 + "        decode(destination_type, 'W', alter_ship_zip_code, ship_zip_code )       = ?      \n "  
 
	  	  */
	  	 
	  	 //CommonDBMethods  commonDBMethods = new CommonDBMethods();
		 List<String> validCountriesList = new ArrayList<>();  
				 
		 try {
			 validCountriesList = commonDBMethods.getStfvListValues("SYK$VALID_COUNTRY_NAME", "VALID_COUNTRY_NAME","!ALL", "!ALL" );
			 LOGGER.info( "ValidCountriesList Cnt(1)={}", validCountriesList.size()); 
		 } catch(Exception e) {
			 LOGGER.error( "ValidCountriesList excep={}", e.getMessage()); 
		 }				 

 
	  	 try(PreparedStatement stmt = inConn.prepareStatement(selectStmt);) {  
 
			if (inConn != null) {

 				stmt.setInt(1, inRunNbr);
 				stmt.setString(2, inEntrySrc);
 				rset = stmt.executeQuery(); 
 				
    			//set oauth api
    			String oAuthApiKey=null;
    			WebtoolsAddrApi webtoolsAddrApi= new WebtoolsAddrApi(); 
    			webtoolsAddrApi.setDbConn(inConn); 
    			webtoolsAddrApi.setoAuthServiceName("WTADDR_OAUTH"); 
    			
    			try {
    				webtoolsAddrApi.getOauthInfo();
    				oAuthApiKey = webtoolsAddrApi.getoAuthApiKey();  
	    			LOGGER.info("Retrieved token ..");
    			} catch (Exception e) {
    				LOGGER.trace("Exception Retrieving token ={}", e.getMessage());
    			} 
    			 
    			LOGGER.info("oAuth key retrieved={}", oAuthApiKey);
 				
    			
    			//set address api service
    			DBUtilsDAO dbUtilsAddr = new DBUtilsDAO(); 
				dbUtilsAddr.setDbConnection(inConn); 
				WSConfigHdrInfoTO  addrWSConfigHdrInfo = new WSConfigHdrInfoTO();
				
			 	try { 
					addrWSConfigHdrInfo = dbUtilsAddr.getWebserviceInfo("WTADDR_API");   
	    			webtoolsAddrApi.setWsConfigHdrInfo(addrWSConfigHdrInfo);
	    			webtoolsAddrApi.setoAuthApiKey(oAuthApiKey);
	    			webtoolsAddrApi.setDbConn(inConn);  
				}catch (Exception e) {
					LOGGER.trace("Exception Setting address service ={}", e.getMessage());
				} 

			 	if (addrWSConfigHdrInfo !=null || oAuthApiKey !=null) { 
			 		
					while (rset.next()) {
						
						LOGGER.trace("Country Code(1) ={}", rset.getString("ship_country"));
						
						if (StringUtils.ignoreCaseAndSpacesContains(validCountriesList,rset.getString("ship_country"))) {
							
							String errMsg = "";
		    				String dlvryPt = "";
		    				String city = "";
		    				String state = "";
		    				String zip = "";
		    				
		    				AddressTO retAddrErrRec = new AddressTO();
		    				AddressAddInfoTO  addressAddInfoRec = new AddressAddInfoTO();
		    				
		    				//set source address  
		    				SrcAddressTO inAddrSrc = new SrcAddressTO();
		    				inAddrSrc.setAddressLine1(rset.getString("ship_addr1"));
		    				inAddrSrc.setAddressLine2(rset.getString("ship_addr2"));
		    				inAddrSrc.setAddressLine3(rset.getString("ship_addr3"));
		    				inAddrSrc.setCity(rset.getString("ship_city"));
		    				inAddrSrc.setState(rset.getString("ship_state"));
		    				inAddrSrc.setZipCode(rset.getString("ship_zip5"));  
		    				inAddrSrc.setZipCodePlus4(rset.getString("ship_zip4"));  
		    				
	 						int wsSeqId = rset.getInt("pfdh_seq");
	 						LOGGER.trace("wsSeqId -{}", rset.getInt("pfdh_seq")); 
	 						
	 						
							PreparedStatement stmt1 = null;
							// ResultSet rset1 = null
							int dataUpdateInd;
		
							try {
	
								try { 
			    					webtoolsAddrApi.validateSrcAddress ( inAddrSrc, "Y");
			    					retAddrErrRec = webtoolsAddrApi.getRetAddrErrRec();
			    					addressAddInfoRec = retAddrErrRec.getAddressAddInfoTO();
		        				}catch (Exception e) {
		        					LOGGER.trace("Exception getting address info ={}", e.getMessage());
		        				}  
		
								LOGGER.info( "Address retAddrErrRec.getAddressStatus()-{} ",retAddrErrRec.getAddressStatus());
								
								if (("EXACT MATCH".equalsIgnoreCase(retAddrErrRec.getAddressStatus()) &&
		    							retAddrErrRec.isValidAddressFound())) {  
		
									try {
										LOGGER.trace("Addr Util API status -{}" ,retAddrErrRec.getAddressStatus());
    									dlvryPt = addressAddInfoRec.getDeliveryPoint();
    									state   = retAddrErrRec.getState();
    									city    = retAddrErrRec.getCity(); 
    									
    									if (retAddrErrRec.getZIPPlus4()!=null) {
    										zip = retAddrErrRec.getZIPCode() + retAddrErrRec.getZIPPlus4();
    									} else {
    										zip = retAddrErrRec.getZIPCode();
    									} 
		 
										stmt1 = inConn.prepareStatement(updateDlvryPtStmt);
										stmt1.setString(1, dlvryPt); 
										stmt1.setString(2, dlvryPt); 
										stmt1.setString(3, city);
										stmt1.setString(4, city);
			    		    			stmt1.setString(5, state);
			    		    			stmt1.setString(6, state);
			    		    			stmt1.setString(7, zip);
			    		    			stmt1.setString(8, zip);
			    		    			stmt1.setInt(9, wsSeqId); 
		
										dataUpdateInd = stmt1.executeUpdate();
		
										if (dataUpdateInd > 0) {
											inConn.commit();
										}
		
										stmt1.close();
									} catch (Exception e) { 
										LOGGER.error( ExceptionUtils.getErrorStackTrace(e,"Exception at ship address exact match - " ));
									}finally{
							       	 	try {
							        		 
							        		if ((stmt1 !=null) && (!stmt1.isClosed() ) ) {
							        			stmt1.close();
							        		}
							            }catch(Exception e) {
							            	LOGGER.error(ExceptionUtils.getErrorStackTrace(e),"ignorable Error "); 
							            } 
							        }
		
								} else  {
		
									try(PreparedStatement stmt2 = inConn.prepareStatement(updateErrorStmt);) {  
		
										errMsg = retAddrErrRec.getAddressStatus() + ". ";
		 
										stmt2.setString(1, errMsg);
										stmt2.setString(2, errMsg);
										stmt2.setInt(3, wsSeqId);
										dataUpdateInd = stmt2.executeUpdate();
										
										LOGGER.error("error address pdfhSeqId - {}" , wsSeqId);
										
										if (dataUpdateInd > 0) {
											inConn.commit();
										} 
		
									} catch (Exception e) {
										 
										LOGGER.error("Exception at ship address NOT xact match - {}" , e.getMessage());
									} 
		
								} 
								
								/*
								 else {
									LOGGER.error("retAddrErrRec.getAddressStatus() response is null ");
									LOGGER.error("Error details -{} - {}" , retAddrErrRec.getErrorCode() , retAddrErrRec.getErrorMsg());
								} 
								*/
		
							} catch (Exception e) {
		 
								LOGGER.error( "webtools address call exception -{}" ,e.getMessage());
							} 
							
						} else {
							LOGGER.trace("Skipping address validation as Country code is not listed in SYK$VALID_COUNTRY_NAME");
						} //if (validCountriesList.contains(rset.getString("ship_country"))) {
	 				
					} //while (rset.next()) {
					
	 				rset.close(); 
 				
				} //if (!addrWSConfigHdrInfo.equals(null)|| oAuthApiKey !=null) { 
			 	
 			} //if (inConn != null) {

		} catch (SQLException sql) {
			outReturnMsg = sql.getMessage();
 		} catch (Exception e) {
			outReturnMsg = e.getMessage();
		}finally{
       	 	try {
        		if ((rset !=null) && (!rset.isClosed() ) ) {
        			rset.close();
        		} 
            }catch(Exception e) {
            	LOGGER.error(ExceptionUtils.getErrorStackTrace(e),"ignorable Error "); 
            } 
        }

		return outReturnMsg;
	}
	
	
	public void setPrintTemplateData(PrintTemplateRec inPrintTemplateRec, String inOrdEntrySrc) {
		
		ResultSet packetTypeFldsResultset = null; 
 		String packetTypeFldsSqlStr="SELECT DISTINCT replace (listagg(nvl(dtl.field_default_value,'#'), ?) within group (order by dtl.tds_position) ,'#','') template_rec "+ 
					                "  FROM printable_packet_map_hdr hdr, \r\n"+
					                "       printable_packet_map_dtl dtl \r\n"+
					                "  WHERE hdr.process_id=? \r\n"+
					                "    AND hdr.application_name=nvl(trim(?),'DEFAULT') \r\n" +  
					                "    AND dtl.ppmh_seq_no=hdr.ppmh_seq_no \r\n"  +
					                "    AND nvl(dtl.ib_active_ind,'N') = 'Y' \r\n" + 
					                "  ORDER BY dtl.tds_position asc  \r\n"  
					            ; 
		 
 		LOGGER.trace("packetTypeFldsSqlStr getIbDelimiterValue()={} ",getIbDelimiterValue());
		LOGGER.trace("packetTypeFldsSqlStr getInterfaceName()={} ",getInterfaceName());
		LOGGER.trace("packetTypeFldsSqlStr inOrdEntrySrc={} ",inOrdEntrySrc);  
		
 		if (getIbDelimiterValue() !=null && inOrdEntrySrc !=null ) {  
 			
 			try(PreparedStatement packetDestTypePreparedStmt = getDbConn().prepareStatement(packetTypeFldsSqlStr);) {   
 				
 				packetDestTypePreparedStmt.setString(1, getIbDelimiterValue());
 				packetDestTypePreparedStmt.setString(2, getInterfaceName());
 				packetDestTypePreparedStmt.setString(3, inOrdEntrySrc);  
		 			
 				LOGGER.trace("packetTypeFldsSqlStr={} ",packetTypeFldsSqlStr);   
 				packetTypeFldsResultset=packetDestTypePreparedStmt.executeQuery();
 				 
 				while (packetTypeFldsResultset.next()) {  
 					setPrintImgTemplateRec(packetTypeFldsResultset.getString("template_rec"));  
	 	 			LOGGER.trace("Image template_rec={} ",packetTypeFldsResultset.getString("template_rec"));  
 				}  	
 
 	  		}catch (Exception e) { 
 	  			LOGGER.error(  ExceptionUtils.getErrorStackTrace(e,"setPrintTemplateData "));
 	 		}finally{
 	    	  	try {
 	    	  		if ((packetTypeFldsResultset !=null) && (  !packetTypeFldsResultset.isClosed() ) ){
 	    	  			packetTypeFldsResultset.close();
 	    	  		} 			
 	    	  	}catch(Exception e) {
 	    	  		LOGGER.info( ExceptionUtils.getErrorStackTrace(e,"Ignore"));
 	    	  	} 
 			} 
  		}   
	}
	
	
	public void printOrMove(String inPrintMoveFlag,  // P-Print, M-Move, B-Both
							String inFileNameWithPath,
							String inFileName, 
							String inDestFilePath, 
							String inDestPrinterName,
							String inAppendDestTstamp // Y default, N
							) {
		
		LOGGER.trace( "End of printOrMove");
		String lpCmdStr=null;
    	String mvCmdStr=null;
    	int runStatus=0;
    	String destFileNameWithPath=null; 
    	String fileDateStr=null;
    	
    	LOGGER.trace( "inPrintMoveFlag    :{}", inPrintMoveFlag);
    	LOGGER.trace( "inFileNameWithPath :{}", inFileNameWithPath);
    	LOGGER.trace( "inFileName         :{}", inFileName);
    	LOGGER.trace( "inDestFilePath     :{}", inDestFilePath);
    	LOGGER.trace( "inDestPrinterName  :{}", inDestPrinterName);
    	LOGGER.trace( "inAppendDestTstamp :{}", inAppendDestTstamp);
	
    	try {
			if (inFileNameWithPath !=null 
					&& inDestPrinterName !=null 
					&& ("P".equalsIgnoreCase(inPrintMoveFlag) || "B".equalsIgnoreCase(inPrintMoveFlag))) {
				
		 		lpCmdStr="lp -d"+inDestPrinterName+" "+inFileNameWithPath;
		 		LOGGER.info( "print command={} ",lpCmdStr); 
		    	runStatus=fileUtils.execHostCommand(lpCmdStr);
		    	LOGGER.info( "print command={} status={}",lpCmdStr,runStatus); 
				
		 	}
			if (inFileNameWithPath !=null 
					&& inDestFilePath !=null 
					&& ("M".equalsIgnoreCase(inPrintMoveFlag) || "B".equalsIgnoreCase(inPrintMoveFlag))) {
				
				destFileNameWithPath=inDestFilePath+inFileName;
				
				if (!"N".equalsIgnoreCase(inAppendDestTstamp)) { 
					fileDateStr="."+DateUtils.getDateTimeStr("yyyyMMddhhmmss");
				}
				
				mvCmdStr="mv "+inFileNameWithPath+" "+destFileNameWithPath+fileDateStr;	 			
				LOGGER.info( "move command={} ",mvCmdStr);
				runStatus=fileUtils.execHostCommand(mvCmdStr);
				LOGGER.info( "move command={} status={}",mvCmdStr,runStatus);
			}
			
    	}catch(Exception e) {
    		LOGGER.error( ExceptionUtils.getErrorStackTrace(e));
    	}
    	
    	LOGGER.trace( "End of printOrMove");
	} 
	  
 	public static void main(String[] args) {
	
		String interfaceNumStr="";
		String connectionNameStr="";
		String fileOptStr="";
		String ordEntrySrc=""; //="USPISANP"
		String addresssValidationInd ="N";
		String dupRecordsValidationInd ="Y";
		//String ignoreZipAddress ="N"
		String errRecordDelimeter ="|";
		String errFileRequired ="N";
		String multipleErrsInoneLinePerOrder="N";
		String errDateFormat="yyyyMMdd";
		String keyOrDateAsFirstField="key";
		String packetNoInErrors="N";
		String zeroErrorsFileRequired ="N";
		
		//System.out.println("Library path="+System.getProperty("java.library.path"));
		
		/*
		String javaLibPath = System.getProperty("java.library.path");
        Map<String, String> envVars = System.getenv();
        System.out.println(envVars.get("Path"));
        System.out.println(javaLibPath);
        for (String var : envVars.keySet()) {
            System.err.println("examining " + var);
            if (envVars.get(var).equals(javaLibPath)) {
                System.out.println(var);
            }
        }
        */
		
		 
		boolean continueProcess=false;
		System.out.println("program started");
 		if (args.length<4) {
			LOGGER.error("Missing required fields connection name or interface name or run option  or All ");
		}else {
			continueProcess=true;
			interfaceNumStr= args[0];
			connectionNameStr= args[1];
			fileOptStr= args[2];
			ordEntrySrc= args[3];
			 
			LOGGER.log(SYSTEM_INFO_LEVEL, "interfaceNumStr={} " , interfaceNumStr);
			LOGGER.log(SYSTEM_INFO_LEVEL, "connectionNameStr={} " , connectionNameStr);
			LOGGER.log(SYSTEM_INFO_LEVEL, "fileOptStr={} " , fileOptStr);
			LOGGER.log(SYSTEM_INFO_LEVEL, "ordEntrySrc={} ", ordEntrySrc); 
			
			if (args.length>4) {
				addresssValidationInd= args[4];
				LOGGER.log(SYSTEM_INFO_LEVEL, "addresssValidationInd={} " , addresssValidationInd);
			}
			if (args.length>5) {
				dupRecordsValidationInd= args[5];
				LOGGER.log(SYSTEM_INFO_LEVEL, "dupRecordsValidationInd={} " ,dupRecordsValidationInd);
			}
			
			if (args.length>6) {
				errFileRequired = args[6];
				LOGGER.log(SYSTEM_INFO_LEVEL, "errFileRequired={} " , errFileRequired);
			}
			
			if (args.length>7) {
				errRecordDelimeter = args[7];
				LOGGER.log(SYSTEM_INFO_LEVEL, "errRecordDelimeter={} " ,errRecordDelimeter);
			}
			
			if (args.length>8) {
				multipleErrsInoneLinePerOrder = args[8];
				LOGGER.log(SYSTEM_INFO_LEVEL, "multipleErrorsInoneLinePerOrder={} " , multipleErrsInoneLinePerOrder);
			}
			if (args.length>9) {
				errDateFormat = args[9];
				LOGGER.log(SYSTEM_INFO_LEVEL, "errDateFormat={} " , errDateFormat);
			}
			
			if (args.length>10) {
				keyOrDateAsFirstField = args[10];
				LOGGER.log(SYSTEM_INFO_LEVEL, "keyOrDateAsFirstField={} ", keyOrDateAsFirstField);
			}
			
			if (args.length>11) {
				packetNoInErrors = args[11]; 
				LOGGER.log(SYSTEM_INFO_LEVEL, "packetNoInErrors={} ", packetNoInErrors);
				
				if ("Y".equalsIgnoreCase(packetNoInErrors) || "YES".equalsIgnoreCase(packetNoInErrors) ) {
					packetNoInErrors= "Y";
				}
				if ("N".equalsIgnoreCase(packetNoInErrors) || "NO".equalsIgnoreCase(packetNoInErrors) ||  "".equalsIgnoreCase(packetNoInErrors)) {
					packetNoInErrors= "N";
				}
			}
			if (args.length>12) {
				zeroErrorsFileRequired = args[12];
				LOGGER.log(SYSTEM_INFO_LEVEL, "zeroErrorsFileRequired={} ", zeroErrorsFileRequired);
			}
			 
		}
	
	
	 	/*
		interfaceNumStr="106-19"
		connectionNameStr="xdhzc0_dmdims0"
		fileOptStr="1"
		continueProcess=true
		//106-19  NAOFA-AR
		//106-20  SSNMM
		//106-21  SEAM
		//106-22  USPISANP

		*/
 	
 		if (continueProcess){
			LoadAndProcessFile loadAndProcessFile = new LoadAndProcessFile();
			//loadAndProcessFile.runProcess(interfaceNumStr,connectionNameStr,fileOptStr,ordEntrySrc,addresssValidationInd, dupRecordsValidationInd,ignoreZipAddress);//,delimiterTypeStr,delimiterStr,enclosedBy,hdrIncludedFlag,positionOrNameBased,null,10 )
			loadAndProcessFile.runProcess(interfaceNumStr,connectionNameStr,fileOptStr,ordEntrySrc,addresssValidationInd, 
					                     dupRecordsValidationInd,errFileRequired,errRecordDelimeter,multipleErrsInoneLinePerOrder,
					                     errDateFormat,keyOrDateAsFirstField,packetNoInErrors,zeroErrorsFileRequired);
		}
		 
		System.out.println("program ended");
	}  //main
	
 	
}
