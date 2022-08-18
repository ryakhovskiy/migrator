package org.kr.db.migrator.reader.settings;

import org.kr.db.migrator.exceptions.ReadSettingsException;
import org.kr.db.migrator.settings.ConnectionInfo;
import org.kr.db.migrator.settings.QueryBase;
import org.kr.db.migrator.settings.XmlSettingsReaderBase;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathConstants;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 28.06.13
 * Time: 16:07
 * To change this template use File | Settings | File Templates.
 */
public class SettingsReader extends XmlSettingsReaderBase<Settings.Query> {

    private static final String FILE = "reader.xml";
    private final Logger log = Logger.getLogger(SettingsReader.class);

    public static Settings readAllSettings() throws ReadSettingsException {
        SettingsReader reader = new SettingsReader();
        Settings settings = reader.readSettings();
        return settings;
    }

    private SettingsReader() throws ReadSettingsException {
        super(FILE);
    }

    private Settings readSettings() throws ReadSettingsException {
        ConnectionInfo connectionInfo = readConnectionInfo();
        List<Settings.Query> queryList = readActions();
        return new Settings(connectionInfo, queryList);
    }

    protected Settings.Query parseQueryNode(Node queryNode) throws ReadSettingsException {
        Element queryXmlElement = (Element)queryNode;
        String strType = getAttributeValue(queryXmlElement, "type");
        QueryBase.QueryType type = QueryBase.QueryType.valueOf(strType.toUpperCase());
        int batchSize = getAttributeIntValue(queryXmlElement, "batch_size");
        int fetchSize = getAttributeIntValue(queryXmlElement, "fetch_size");
        String queueTopic = getAttributeValue(queryXmlElement, "queue_topic");
        String threadName = getAttributeValue(queryXmlElement, "thread_name");
        int queueCapacity = getAttributeIntValue(queryXmlElement, "queue_capacity");
        String sql = getSqlQuery(queryNode);
        return new Settings.Query(type, fetchSize, batchSize, queueTopic, sql, threadName, queueCapacity);
    }

    private String getSqlQuery(Node queryNode) throws ReadSettingsException {
        Node sqlNode = (Node)evaluateXPath("./sql", queryNode, XPathConstants.NODE);
        String sqlQuery = sqlNode.getTextContent();
        return sqlQuery;
    }

    protected Settings.Query parseTableNode(Node tableNode) throws ReadSettingsException {
        Element tableXmlElement = (Element)tableNode;
        QueryBase.QueryType type = QueryBase.QueryType.SELECT;
        int batchSize = getAttributeIntValue(tableXmlElement, "batch_size");
        int fetchSize = getAttributeIntValue(tableXmlElement, "fetch_size");
        String queueTopic = getAttributeValue(tableXmlElement, "queue_topic");
        String tableName =  getAttributeValue(tableXmlElement, "name");
        String threadName = getAttributeValue(tableXmlElement, "thread_name");
        int queueCapacity = getAttributeIntValue(tableXmlElement, "queue_capacity");
        String sql = createSqlQuery(tableNode, tableName);
        return new Settings.Query(type, fetchSize, batchSize, queueTopic, sql, threadName, queueCapacity);
    }

    private String createSqlQuery(Node tableNode, String tableName) throws ReadSettingsException {
        NodeList nodeList = (NodeList)evaluateXPath("./column", tableNode, XPathConstants.NODESET);
        StringBuilder queryBuilder = new StringBuilder("select ");
        for (int i = 0; i < nodeList.getLength(); i++) {
            String columnName = getAttributeValue((Element)nodeList.item(i), "name");
            queryBuilder.append(columnName);
            queryBuilder.append(",");
        }
        //delete last comma
        queryBuilder.deleteCharAt(queryBuilder.length() - 1);
        queryBuilder.append(" from ");
        queryBuilder.append(tableName);
        return queryBuilder.toString();
    }
}
