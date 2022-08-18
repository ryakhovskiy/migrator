package org.kr.db.migrator.settings;

import org.kr.db.migrator.exceptions.ReadSettingsException;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: I005144
 * Date: 04.07.13
 * Time: 16:50
 * To change this template use File | Settings | File Templates.
 */
public abstract class XmlReaderBase {

    private final Logger log = Logger.getLogger(XmlReaderBase.class);
    private final Document document;
    private final XPathFactory xPathFactory = XPathFactory.newInstance();

    public XmlReaderBase(String file) throws ReadSettingsException {
        try {
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            document = documentBuilder.parse(new File(file));
        } catch (ParserConfigurationException e) {
            log.error("Cannot create DocumentBuilder instance.", e);
            throw new ReadSettingsException();
        } catch (SAXException e) {
            log.error("Non-XML content in the file " + file, e);
            throw new ReadSettingsException();
        } catch (IOException e) {
            log.error("Error while reading file " + file, e);
            throw new ReadSettingsException();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new ReadSettingsException(e);
        }
    }

    protected int getAttributeIntValue(Element xmlElement, String attributeName) throws ReadSettingsException {
        try {
            return Integer.valueOf(getAttributeValue(xmlElement, attributeName));
        } catch (NumberFormatException e) {
            throw new ReadSettingsException("Attribute " + attributeName + " is non-integer", e);
        }
    }

    protected String getAttributeValue(Element xmlElement, String attributeName) throws ReadSettingsException {
        if (!xmlElement.hasAttribute(attributeName))
            throw new ReadSettingsException(String.format("Error while reading %s, no %s attribute", xmlElement.getNodeName(), attributeName));
        return xmlElement.getAttribute(attributeName);
    }

    protected String getAttributeValueIfExists(Element xmlElement, String attributeName) {
        if (!xmlElement.hasAttribute(attributeName)) {
            log.debug("attribute does not exists: " + attributeName);
            return "";
        }
        else return xmlElement.getAttribute(attributeName);
    }

    protected int getIntAttributeValueIfExists(Element xmlElement, String attributeName) {
        if (!xmlElement.hasAttribute(attributeName)) {
            log.debug("attribute does not exists: " + attributeName);
            return 0;
        }
        else return Integer.valueOf(xmlElement.getAttribute(attributeName));
    }

    protected Object evaluateXPath(String xPathExpression, QName returnType) throws ReadSettingsException {
        return evaluateXPath(xPathExpression, document, returnType);
    }

    protected Object evaluateXPath(String xPathExpression, Object item, QName returnType) throws ReadSettingsException {
        XPath xPath = xPathFactory.newXPath();
        try {
            return xPath.evaluate(xPathExpression, item, returnType);
        } catch (XPathExpressionException e) {
            log.error("Error while evaluating xPath expression: " + xPathExpression, e);
            throw new ReadSettingsException(e);
        }
    }

}
