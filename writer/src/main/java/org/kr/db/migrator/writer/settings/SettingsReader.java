package org.kr.db.migrator.writer.settings;

import org.kr.db.migrator.exceptions.ReadSettingsException;
import org.kr.db.migrator.settings.ConnectionInfo;
import org.kr.db.migrator.settings.QueryBase;
import org.kr.db.migrator.settings.SettingsBase;
import org.kr.db.migrator.settings.XmlSettingsReaderBase;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPathConstants;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 01.07.13
 * Time: 11:27
 * To change this template use File | Settings | File Templates.
 */
public class SettingsReader extends XmlSettingsReaderBase<Settings.Query> {

    private final static String FILE = "writer.xml";
    private final Logger log = Logger.getLogger(SettingsReader.class);

    public SettingsReader() throws ReadSettingsException {
        super(FILE);
    }

    public static Settings readAllSettings() throws ReadSettingsException {
        SettingsReader reader = new SettingsReader();
        SettingsBase settingsBase = reader.readSettings();
        return (Settings)settingsBase;
    }

    private Settings readSettings() throws ReadSettingsException {
        ConnectionInfo connectionInfo = readConnectionInfo();
        List<Settings.Query> queryList = readActions();
        return new Settings(connectionInfo, queryList);
    }

    @Override
    protected Settings.Query parseQueryNode(Node queryNode) throws ReadSettingsException {
        Element queryXmlElement = (Element)queryNode;
        String strType = getAttributeValue(queryXmlElement, "type");
        QueryBase.QueryType type = QueryBase.QueryType.valueOf(strType.toUpperCase());
        String queueTopic = getAttributeValue(queryXmlElement, "queue_topic");
        int commitSize = getAttributeIntValue(queryXmlElement, "commit_size");
        int jmsReaders = getAttributeIntValue(queryXmlElement, "jms_readers");
        int sqlWriters = getAttributeIntValue(queryXmlElement, "sql_writers");
        int queueCapacity = getAttributeIntValue(queryXmlElement, "queue_capacity");
        String sql = getSqlQuery(queryNode);
        Settings.Query query = new Settings.Query(type, commitSize, queueTopic, sql, jmsReaders, sqlWriters, queueCapacity);
        addArguments(queryNode, query);
        return query;
    }

    private void addArguments(Node queryNode, Settings.Query query) throws ReadSettingsException {
        NodeList argumentNodes = (NodeList)evaluateXPath("./arguments/argument", queryNode, XPathConstants.NODESET);
        for (int i = 0; i < argumentNodes.getLength(); i++) {
            Element argumentElement = (Element)argumentNodes.item(i);
            int argIndex = getAttributeIntValue(argumentElement, "argIndex");
            String strType = getAttributeValue(argumentElement, "type");
            Settings.Query.ArgumentType type = Settings.Query.ArgumentType.valueOf(strType.toUpperCase());
            String valueSource = getAttributeValue(argumentElement, "value_source");
            query.addArgument(argIndex, type, valueSource);
        }
    }

    private String getSqlQuery(Node queryNode) throws ReadSettingsException {
        Node sqlNode = (Node)evaluateXPath("./sql", queryNode, XPathConstants.NODE);
        String sqlQuery = sqlNode.getTextContent();
        return sqlQuery;
    }

    @Override
    protected Settings.Query parseTableNode(Node tableNode) throws ReadSettingsException {
        Element tableXmlElement = (Element)tableNode;
        QueryBase.QueryType queryType = QueryBase.QueryType.INSERT;
        String queueTopic = getAttributeValue(tableXmlElement, "queue_topic");
        int commitSize = getAttributeIntValue(tableXmlElement, "commit_size");
        int jmsReaders = getAttributeIntValue(tableXmlElement, "jms_readers");
        int sqlWriters = getAttributeIntValue(tableXmlElement, "sql_writers");
        int queueCapacity = getAttributeIntValue(tableXmlElement, "queue_capacity");
        String tableName = getAttributeValue(tableXmlElement, "name");
        NodeList columnNodes = (NodeList)evaluateXPath("./column",tableNode, XPathConstants.NODESET);
        List<Settings.Query.QueryArgument> argumentList = new ArrayList<Settings.Query.QueryArgument>();
        int argIndex = 1;
        StringBuilder queryBuilder = new StringBuilder("insert into ");
        queryBuilder.append(tableName);
        queryBuilder.append(" (");
        for (int i = 0; i < columnNodes.getLength(); i++) {
            Element columnElement = (Element)columnNodes.item(i);
            String name = getAttributeValue(columnElement, "name");
            String strType = getAttributeValue(columnElement, "type");
            Settings.Query.ArgumentType argType = Settings.Query.ArgumentType.valueOf(strType.toUpperCase());
            String valueSource = getAttributeValue(columnElement, "value_source");
            Settings.Query.QueryArgument argument = new Settings.Query.QueryArgument(argIndex++, argType, valueSource);
            argumentList.add(argument);
            queryBuilder.append(name);
            queryBuilder.append(",");
        }
        //delete last comma
        queryBuilder.deleteCharAt(queryBuilder.length() - 1);
        queryBuilder.append(") values (");
        for (int i = 0; i < argumentList.size(); i++)
            queryBuilder.append("?,");
        //delete last comma
        queryBuilder.deleteCharAt(queryBuilder.length() - 1);
        queryBuilder.append(")");
        Settings.Query query = new Settings.Query(queryType, commitSize, queueTopic, queryBuilder.toString(), jmsReaders, sqlWriters, queueCapacity);
        query.setArguments(argumentList);
        return query;
    }

}
