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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.doc.tools;

import org.neo4j.test.TestData.Producer;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

/**
 * This class is supporting the generation of ASCIIDOC documentation
 * from Java JUnit tests. Snippets can be supplied programmatically in the Java-section
 * and will replace their @@snippetName placeholders in the documentation description.
 */
public class JavaTestDocsGenerator extends AsciiDocGenerator {
    public static final Producer<JavaTestDocsGenerator> PRODUCER = (graph, title, documentation) -> (JavaTestDocsGenerator) new JavaTestDocsGenerator(title).description(documentation);

    public JavaTestDocsGenerator(String title) {
        super(title, "docs");
    }

    public void document(String directory, String sectionName) {
        this.setSection(sectionName);
        String name = title.replace(" ", "-").toLowerCase();
        File dir = new File(new File(directory), section);
        String filename = name + ".asciidoc";
        Writer fw = getFW(dir, filename);
        description = replaceSnippets(description, dir, name);
        try {
            line(fw, "[[" + sectionName + "-" + name.replaceAll("[()]", "") + "]]");
            String firstChar = title.substring(0, 1).toUpperCase();
            line(fw, firstChar + title.substring(1));
            for (int i = 0; i < title.length(); i++) {
                fw.append("=");
            }
            fw.append("\n");
            line(fw, "");
            line(fw, description);
            line(fw, "");
            fw.flush();
            fw.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void addImageSnippet(String tagName, String imageName, String title) {
        this.addSnippet(tagName, "\nimage:" + imageName + "[" + title + "]\n");
    }
}
