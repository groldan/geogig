package org.locationtech.geogig.cli.porcelain;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;

import java.io.IOException;
import java.io.OutputStream;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public class GraphMLPrinter {

    private XMLStreamWriter w;

    private OutputStream stream;

    public GraphMLPrinter(OutputStream stream) {
        this.stream = stream;
    }

    public void start() {
        XMLOutputFactory factory = XMLOutputFactory.newFactory();
        try {
            w = factory.createXMLStreamWriter(stream, "UTF-8");

            w.writeStartDocument("UTF-8", "1.0");
            startElement("graphml");
            att("xmlns", "http://graphml.graphdrawing.org/xmlns");
            att("xmlns", "xsi", "http://www.w3.org/2001/XMLSchema-instance");
            att("xmlns", "y", "http://www.yworks.com/xml/graphml");
            att("xmlns", "yed", "http://www.yworks.com/xml/yed/3");
            att("xsi", "schemaLocation",
                    "http://graphml.graphdrawing.org/xmlns http://www.yworks.com/xml/schema/graphml/1.1/ygraphml.xsd");

            // <key for="node" id="nd" yfiles.type="nodegraphics"/>
            startElement("key");
            att("for", "node");
            att("id", "nd");
            att("yfiles.type", "nodegraphics");
            endElement();

            // <key for="edge" id="ed" yfiles.type="nodegraphics"/>
            startElement("key");
            att("for", "edge");
            att("id", "ed");
            att("yfiles.type", "nodegraphics");
            endElement();

            startElement("graph");
            att("edgedefault", "directed");

        } catch (XMLStreamException e) {
            throw propagate(e);
        }
    }

    private boolean inData, inEdge, inNode;

    public GraphMLPrinter startData() {
        checkState(inNode || inEdge);
        inData = true;
        startElement("data");
        if (inEdge) {
            att("key", "ed");
        } else {
            att("key", "nd");
        }
        startElement("y", "ShapeNode");
        return this;
    }

    public GraphMLPrinter endData() {
        inData = false;
        endElement();// y:ShapeNode
        endElement();// key
        return this;
    }

    public GraphMLPrinter att(String localName, String value) {
        try {
            w.writeAttribute(localName, value);
        } catch (XMLStreamException e) {
            propagate(e);
        }
        return this;
    }

    public GraphMLPrinter att(String prefix, String localName, String value) {
        try {
            w.writeAttribute(prefix, "", localName, value);
        } catch (XMLStreamException e) {
            propagate(e);
        }
        return this;
    }

    public GraphMLPrinter end() {
        try {
            endElement();// graph
            endElement();// grpahml
            w.writeEndDocument();
            stream.flush();
        } catch (XMLStreamException | IOException e) {
            throw propagate(e);
        }
        return this;
    }

    public GraphMLPrinter startElement(String localName) {
        return startElement("", localName);
    }

    public GraphMLPrinter startElement(String prefix, String localName) {
        try {
            w.writeStartElement(prefix, localName, "");
        } catch (XMLStreamException e) {
            propagate(e);
        }
        return this;
    }

    public GraphMLPrinter endElement() {
        try {
            w.writeEndElement();
        } catch (XMLStreamException e) {
            propagate(e);
        }
        return this;
    }

    public GraphMLPrinter node(String id) {
        startNode(id);
        endNode();
        return this;
    }

    public GraphMLPrinter startNode(String id) {
        checkState(!inNode);
        inNode = true;
        startElement("node");
        att("id", id);
        return this;
    }

    public GraphMLPrinter endNode() {
        checkState(inNode);
        inNode = false;
        endElement();
        return this;
    }

    public GraphMLPrinter startEdge(String from, String to) {
        checkState(!inEdge);
        inEdge = true;
        startElement("edge");
        att("source", from);
        att("target", to);
        return this;
    }

    public GraphMLPrinter endEdge() {
        checkState(inEdge);
        inEdge = false;
        endElement();
        return this;
    }

    public GraphMLPrinter edge(String from, String to) {
        return startEdge(from, to).endEdge();
    }

    public GraphMLPrinter label(String label) {
        checkState(inData);
        if (inNode) {
            return startElement("y", "NodeLabel").characters(label).endElement();
        }
        return startElement("y", "PolyLineEdge")//
                .startElement("y", "EdgeLabel")//
                .characters(label)//
                .endElement()//
                .endElement();
    }

    public GraphMLPrinter shapeEllipse() {
        return shape("ellipse");
    }

    public GraphMLPrinter shapeRectangle() {
        return shape("rectangle");
    }

    public GraphMLPrinter shapeRoundRectangle() {
        return shape("roundrectangle");
    }

    public GraphMLPrinter shapeDiamond() {
        return shape("diamond");
    }

    public GraphMLPrinter shapeParallelogram() {
        return shape("parallelogram");
    }

    public GraphMLPrinter shapeHexagon() {
        return shape("hexagon");
    }

    public GraphMLPrinter shapeTrapezoid() {
        return shape("trapezoid");
    }

    private GraphMLPrinter shape(String shapename) {
        checkState(inData);
        return startElement("y", "Shape").att("type", shapename).endElement();
    }

    public GraphMLPrinter characters(String content) {
        try {
            w.writeCharacters(content);
        } catch (XMLStreamException e) {
            propagate(e);
        }
        return this;
    }
}
