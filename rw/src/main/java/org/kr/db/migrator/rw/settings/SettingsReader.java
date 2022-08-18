package org.kr.db.migrator.rw.settings;

import org.kr.db.migrator.exceptions.ReadSettingsException;
import org.kr.db.migrator.settings.*;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathConstants;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 04.07.13
 * Time: 14:20
 * To change this template use File | Settings | File Templates.
 */
public class SettingsReader extends XmlReaderBase {

    private final Logger log = Logger.getLogger(SettingsReader.class);
    private static final String FILE = "rw.xml";

    public static Settings readSettigns() throws ReadSettingsException {
        return new SettingsReader(FILE).readAllSettings();
    }

    public static Settings readSettigns(String file) throws ReadSettingsException {
        return new SettingsReader(file).readAllSettings();
    }

    private SettingsReader(String file) throws ReadSettingsException {
        super(file);
    }

    private Settings readAllSettings() throws ReadSettingsException {
        try {
            ConnectionInfo connectionInfo = readConnectionInfo();
            List<Action> actions = readActions(connectionInfo);
            return new Settings(connectionInfo, actions);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new ReadSettingsException(e);
        }
    }

    private ConnectionInfo readConnectionInfo() throws ReadSettingsException {
        log.debug("reading connection info...");
        ConnectionInfo.DatabaseConnectionInfo sourceConnectionInfo = readDatabaseConnectionInfo("/settings/connection/source_database");
        ConnectionInfo.DatabaseConnectionInfo destinationConnectionInfo = readDatabaseConnectionInfo("/settings/connection/destination_database");
        try {
            initDrivers(sourceConnectionInfo);
            initDrivers(destinationConnectionInfo);
        } catch (ClassNotFoundException e) {
            throw new ReadSettingsException(e);
        }
        return new ConnectionInfo(sourceConnectionInfo, destinationConnectionInfo);
    }

    private void initDrivers(ConnectionInfo.DatabaseConnectionInfo connectionInfo) throws ClassNotFoundException {
        log.debug("initializing driver: " + connectionInfo.getDriver());
        Class.forName(connectionInfo.getDriver());
    }

    private ConnectionInfo.DatabaseConnectionInfo readDatabaseConnectionInfo(String path) throws ReadSettingsException {
        Element xmlElement = (Element)evaluateXPath(path, XPathConstants.NODE);
        String driver = getAttributeValue(xmlElement, "driver");
        String url = getAttributeValue(xmlElement, "url");
        return new ConnectionInfo.DatabaseConnectionInfo(driver, url);
    }

    private List<Action> readActions(ConnectionInfo connectionInfo) throws ReadSettingsException {
        log.debug("reading actions...");
        List<Action> actions = new ArrayList<Action>();
        List<Action> tableActions = readTables(connectionInfo);
        List<Action> queryActions = readQueries();
        actions.addAll(tableActions);
        actions.addAll(queryActions);
        return actions;
    }

    private List<Action> readTables(ConnectionInfo connectionInfo) throws ReadSettingsException {
        log.debug("reading tables...");
        List<Action> actions = new ArrayList<Action>();
        Node tablesNode = (Node)evaluateXPath("/settings/actions/tables", XPathConstants.NODE);
        TableSettings commonTableSettings = new TableSettings();
        readTableCommonSettings((Element)tablesNode, commonTableSettings);
        NodeList tableNodes = (NodeList)evaluateXPath("/settings/actions/tables/table", XPathConstants.NODESET);
        for (int i = 0; i < tableNodes.getLength(); i++)
            actions.add(parseTableNode(tableNodes.item(i), commonTableSettings, connectionInfo));
        return actions;
    }

    private List<Action> readQueries() throws ReadSettingsException {
        log.debug("reading queries...");
        List<Action> actions = new ArrayList<Action>();
        NodeList tableNodes = (NodeList)evaluateXPath("/settings/actions/queries/query", XPathConstants.NODESET);
        for (int i = 0; i < tableNodes.getLength(); i++)
            actions.add(parseQueryNode(tableNodes.item(i)));
        return actions;
    }

    private void readTableCommonSettings(Element xmlElement, TableSettings tableSettings) throws ReadSettingsException {
        log.debug("reading common table settings...");
        int fetchSize = getIntAttributeValueIfExists(xmlElement, "fetch_size");
        if (fetchSize > 0)
            tableSettings.setFetchSize(fetchSize);
        int batchSize = getIntAttributeValueIfExists(xmlElement, "batch_size");
        if (batchSize > 0)
            tableSettings.setBatchSize(batchSize);
        int commitSize = getIntAttributeValueIfExists(xmlElement, "commit_size");
        if (commitSize > 0)
            tableSettings.setCommitSize(commitSize);
        int sqlWriters = getIntAttributeValueIfExists(xmlElement, "writers");
        if (sqlWriters > 0)
            tableSettings.setSqlWriters(sqlWriters);
        int queueCapacity = getIntAttributeValueIfExists(xmlElement, "queue_capacity");
        if (queueCapacity > 0)
            tableSettings.setQueueCapacity(queueCapacity);
        String sourceOpenEscapeSymbol = getAttributeValueIfExists(xmlElement, "source_open_escape_symbol");
        if (null != sourceOpenEscapeSymbol && sourceOpenEscapeSymbol.length() > 0)
            tableSettings.setSourceOpenEscapeSymbol(sourceOpenEscapeSymbol);
        String sourceCloseEscapeSymbol = getAttributeValueIfExists(xmlElement, "source_close_escape_symbol");
        if (null != sourceCloseEscapeSymbol && sourceCloseEscapeSymbol.length() > 0)
            tableSettings.setSourceCloseEscapeSymbol(sourceCloseEscapeSymbol);
        String destOpenEscapeSymbol = getAttributeValueIfExists(xmlElement, "dest_open_escape_symbol");
        if (null != destOpenEscapeSymbol && destOpenEscapeSymbol.length() > 0)
            tableSettings.setDestOpenEscapeSymbol(destOpenEscapeSymbol);
        String destCloseEscapeSymbol = getAttributeValueIfExists(xmlElement, "dest_close_escape_symbol");
        if (null != destCloseEscapeSymbol && destCloseEscapeSymbol.length() > 0)
            tableSettings.setDestCloseEscapeSymbol(destCloseEscapeSymbol);
    }

    protected Action parseTableNode(Node tableNode, TableSettings commonTableSettings, ConnectionInfo connectionInfo) throws ReadSettingsException {
        Element xmlElement = (Element)tableNode;
        TableSettings particularTableSettings = commonTableSettings.clone();
        readTableCommonSettings(xmlElement, particularTableSettings);
        String sourceTable = getAttributeValue(xmlElement, "source_name");
        String destinationTable = getAttributeValue(xmlElement, "dest_name");
        String threadPrefix = getAttributeValueIfExists(xmlElement, "thread_prefix");
        String where = getAttributeValueIfExists(xmlElement, "where");
        String selectStatement = createSqlSelectStatement(tableNode, sourceTable, particularTableSettings.getSourceOpenEscapeSymbol(), particularTableSettings.getSourceCloseEscapeSymbol(), connectionInfo.getSourceDbConnectionInfo().getUrl());
        if (null != where && where.length() > 0)
            selectStatement = selectStatement + " " + where;
        Map<String, Integer> argMapping = new HashMap<String, Integer>();
        String insertStatement = createSqlInsertStatement(tableNode, destinationTable, argMapping, particularTableSettings.getDestOpenEscapeSymbol(), particularTableSettings.getDestCloseEscapeSymbol(), connectionInfo.getDestinationDbConnectionInfo().getUrl());
        QueryRead queryRead = new QueryRead(selectStatement, QueryBase.QueryType.SELECT, threadPrefix, particularTableSettings.getFetchSize());
        QueryWrite queryWrite = new QueryWrite(insertStatement, QueryBase.QueryType.INSERT, particularTableSettings.getBatchSize(), threadPrefix, particularTableSettings.getCommitSize(), argMapping);
        return new Action(queryRead, queryWrite, particularTableSettings.getSqlWriters(), particularTableSettings.getQueueCapacity());
    }

    protected Action parseQueryNode(Node queryNode) throws ReadSettingsException {
        Element xmlElement = (Element)queryNode;
        int sqlWriters = getAttributeIntValue(xmlElement, "writers");
        int queueCapacity = getAttributeIntValue(xmlElement, "queue_capacity");
        String threadPrefix = getAttributeValue(xmlElement, "thread_prefix");

        Element sourceElement = (Element)evaluateXPath("./source", queryNode, XPathConstants.NODE);
        String strSourceQueryType = getAttributeValue(sourceElement, "type").toUpperCase();
        QueryBase.QueryType sourceQueryType = QueryBase.QueryType.valueOf(strSourceQueryType);
        int fetchSize = getAttributeIntValue(sourceElement, "fetch_size");
        Node sourceSqlNode = (Node)evaluateXPath("./sql", sourceElement, XPathConstants.NODE);
        String sourceSqlStatement = sourceSqlNode.getTextContent();

        Element destinationElement = (Element)evaluateXPath("./destination", queryNode, XPathConstants.NODE);
        //type="insert" batch_size="1000" commit_size="10000"
        String strDestinationQueryType = getAttributeValue(destinationElement, "type").toUpperCase();
        QueryBase.QueryType destinationQueryType = QueryBase.QueryType.valueOf(strDestinationQueryType);
        int batchSize = getAttributeIntValue(destinationElement, "batch_size");
        int commitSize = getAttributeIntValue(destinationElement, "commit_size");
        Node destinationSqlNode = (Node)evaluateXPath("./sql", destinationElement, XPathConstants.NODE);
        String destinationSqlStatement = destinationSqlNode.getTextContent();
        Map<String, Integer> argMapping = getArgumentsMapping(destinationElement);

        QueryRead queryRead = new QueryRead(sourceSqlStatement, sourceQueryType, threadPrefix, fetchSize);
        QueryWrite queryWrite = new QueryWrite(destinationSqlStatement, destinationQueryType, batchSize, threadPrefix, commitSize, argMapping);
        return new Action(queryRead, queryWrite, sqlWriters, queueCapacity);
    }

    private String createSqlSelectStatement(Node tableNode, String tableName, String sourceOpenEscapeSymbol, String sourceCloseEscapeSymbol, String url) throws ReadSettingsException {
        NodeList nodeList = (NodeList)evaluateXPath("./column", tableNode, XPathConstants.NODESET);
        if (nodeList.getLength() == 0) {
            try {
                return QueryBuilder.newInstance().createSelectStatement(url, tableName, sourceOpenEscapeSymbol, sourceCloseEscapeSymbol);
            } catch (SQLException e) {
                throw new ReadSettingsException("Error while reading table metadata", e);
            }
        }

        //else:
        String[] columns = new String[nodeList.getLength()];
        for (int i = 0; i < nodeList.getLength(); i++)
            columns[i] = getAttributeValue((Element)nodeList.item(i), "source_name");
        return QueryBuilder.newInstance().createSelectStatement(tableName, columns,sourceOpenEscapeSymbol, sourceCloseEscapeSymbol);
    }

    private String createSqlInsertStatement(Node tableNode, String tableName, Map<String, Integer> argMapping, String destOpenEscapeSymbol, String destCloseEscapeSymbol, String url) throws ReadSettingsException {
        NodeList nodeList = (NodeList)evaluateXPath("./column", tableNode, XPathConstants.NODESET);
        argMapping.clear();
        if (nodeList.getLength() == 0) {
            try {
                return QueryBuilder.newInstance().createInsertStatement(url, tableName, destOpenEscapeSymbol, destCloseEscapeSymbol, argMapping);
            } catch (SQLException e) {
                throw new ReadSettingsException("Error while reading table metadata", e);
            }
        }
        String[] columns = new String[nodeList.getLength()];
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element columnElement = (Element)nodeList.item(i);
            columns[i] = getAttributeValue(columnElement, "dest_name");
            //String sourceColumn = getAttributeValue(columnElement, "source_name");
        }
        return QueryBuilder.newInstance().createInsertStatement(tableName, columns, destOpenEscapeSymbol, destCloseEscapeSymbol, argMapping);
    }

    private Map<String, Integer> getArgumentsMapping(Node destinationNode) throws ReadSettingsException {
        if (!destinationNode.getNodeName().equals("destination"))
            throw new IllegalArgumentException("Node is not 'destination'");
        Map<String, Integer> argMapping = new HashMap<String, Integer>();
        NodeList nodeList = (NodeList)evaluateXPath("./arguments/argument", destinationNode, XPathConstants.NODESET);
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element xmlElement = (Element)nodeList.item(i);
            int argIndex = getAttributeIntValue(xmlElement, "argIndex");
            String source = getAttributeValue(xmlElement, "value_source");
            argMapping.put(source, argIndex);
        }
        return argMapping;
    }
}
