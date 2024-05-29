/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Spatial.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.doc.tools;

public class GraphVizConfig {

	String nodeFontSize = "10";
	String edgeFontSize = nodeFontSize;
	String nodeFontColor = "#1c2021"; // darker grey
	String edgeFontColor = nodeFontColor;
	String edgeColor = "#2e3436"; // dark grey
	String boxColor = edgeColor;
	String edgeHighlight = "#a40000"; // dark red
	String nodeFillColor = "#ffffff";
	String nodeHighlight = "#fcee7d"; // lighter yellow
	String nodeHighlight2 = "#fcc574"; // lighter orange
	String nodeShape = "box";

	// Commandline args in the shell script, in order
	String fontPath;
	// We are not calling the dot command from here at the moment, so we don't need the filename
	// String targetImage;
	String colorSet;
	String graphAttrs;

	String graphSettings = "graph [size=\"7.0,9.0\" fontpath=\"" + fontPath + "\"]";
	String nodeStyle = "filled,rounded";
	String nodeSep = "0.4";

	String textNode = "shape=plaintext,style=diagonals,height=0.2,margin=0.0,0.0";

	String arrowHead = "vee";
	String arrowSize = "0.75";

	String inData;

	String graphFont = "FreeSans";
	String nodeFont = graphFont;
	String edgeFont = graphFont;

	public GraphVizConfig(String inData, String fontPath, String colorSet, String graphAttrs) {
		this.fontPath = fontPath;
		this.colorSet = colorSet;
		this.graphAttrs = graphAttrs;

		handleColorSet(colorSet);

		this.inData = handleInData(inData);
	}

	public String get() {
		String prepend = String.format("digraph g{ %s ", graphSettings) +
				String.format("node [shape=\"%s\" penwidth=1.5 fillcolor=\"%s\" color=\"%s\" ", nodeShape,
						nodeFillColor, boxColor) +
				String.format("fontcolor=\"%s\" style=\"%s\" fontsize=%s fontname=\"%s\"] ", nodeFontColor, nodeStyle,
						nodeFontSize, nodeFont) +
				String.format("edge [color=\"%s\" penwidth=2 arrowhead=\"%s\" arrowtail=\"%s\" ", boxColor, arrowHead,
						arrowHead) +
				String.format("arrowSize=%s fontcolor=\"%s\" fontsize=%s fontname=\"%s\"] ", arrowSize, edgeFontColor,
						edgeFontSize, edgeFont) +
				String.format("nodesep=%s fontname=\"%s\"", nodeSep, graphFont) +
				graphAttrs;

		return prepend + this.inData + "}";
	}

	private final void handleColorSet(String colorSet) {
		if (colorSet.equals("meta")) {
			this.nodeFillColor = "#fadcad";
			this.nodeHighlight = "#a8e270";
			this.nodeHighlight2 = "#95bbe3";
		} else if (colorSet.equals("neoviz")) {
			this.nodeShape = "Mrecord";
			this.nodeFontSize = "8";
			this.edgeFontSize = this.nodeFontSize;
		}
	}

	private final String handleInData(String in) {
		return in.replace("NODEHIGHLIGHT", nodeHighlight)
				.replace("NODE2HIGHLIGHT", nodeHighlight2)
				.replace("EDGEHIGHLIGHT", edgeHighlight)
				.replace("BOXCOLOR", boxColor)
				.replace("TEXTNODE", textNode);
	}


}
