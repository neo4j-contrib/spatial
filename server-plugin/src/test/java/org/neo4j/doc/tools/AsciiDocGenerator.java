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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Generate asciidoc-formatted documentation from HTTP requests and responses.
 * The status and media type of all responses is checked as well as the
 * existence of any expected headers.
 * <p>
 * The filename of the resulting ASCIIDOC test file is derived from the title.
 * <p>
 * The title is determined by either a JavaDoc period terminated first title
 * line, the @Title annotation or the method name, where "_" is replaced by " ".
 */
public abstract class AsciiDocGenerator {

	private static final String DOCUMENTATION_END = "\n...\n";
	private final Logger log = Logger.getLogger(AsciiDocGenerator.class.getName());
	protected final String title;
	protected String section;
	protected String description = null;
	protected GraphDatabaseService graph;
	protected static final String SNIPPET_MARKER = "@@";
	protected Map<String, String> snippets = new HashMap<String, String>();
	private static final Map<String, Integer> counters = new HashMap<String, Integer>();

	public AsciiDocGenerator(final String title, final String section) {
		this.section = section;
		this.title = title.replace("_", " ");
	}

	public AsciiDocGenerator setGraph(GraphDatabaseService graph) {
		this.graph = graph;
		return this;
	}

	public String getTitle() {
		return title;
	}

	public AsciiDocGenerator setSection(final String section) {
		this.section = section;
		return this;
	}

	/**
	 * Add a description to the test (in asciidoc format). Adding multiple
	 * descriptions will yield one paragraph per description.
	 *
	 * @param description the description
	 */
	public AsciiDocGenerator description(final String description) {
		if (description == null) {
			throw new IllegalArgumentException(
					"The description can not be null");
		}
		String content;
		int pos = description.indexOf(DOCUMENTATION_END);
		if (pos != -1) {
			content = description.substring(0, pos);
		} else {
			content = description;
		}
		if (this.description == null) {
			this.description = content;
		} else {
			this.description += "\n\n" + content;
		}
		return this;
	}

	protected static void line(final Writer fw, final String string)
			throws IOException {
		fw.append(string);
		fw.append("\n");
	}

	public static Writer getFW(File dir, String filename) {
		try {
			if (!dir.exists()) {
				dir.mkdirs();
			}
			File out = new File(dir, filename);
			if (out.exists()) {
				out.delete();
			}
			if (!out.createNewFile()) {
				throw new RuntimeException("File exists: "
						+ out.getAbsolutePath());
			}
			return new OutputStreamWriter(new FileOutputStream(out, false), StandardCharsets.UTF_8);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public static String getPath(Class<?> source) {
		return source.getPackage()
				.getName()
				.replace(".", "/") + "/" + source.getSimpleName() + ".java";
	}

	protected String replaceSnippets(String description) {
		for (var entry : snippets.entrySet()) {
			String snippetString = SNIPPET_MARKER + entry.getKey();
			if (description.contains(snippetString + "\n")) {
				description = description.replace(snippetString + "\n", "\n" + entry.getValue());
			} else {
				log.severe("Could not find " + snippetString + "\\n in " + description);
			}
		}
		if (description.contains(SNIPPET_MARKER)) {
			int indexOf = description.indexOf(SNIPPET_MARKER);
			String snippet = description.substring(indexOf, description.indexOf("\n", indexOf));
			log.severe("missing snippet [" + snippet + "] in " + description);
		}
		return description;
	}

	/**
	 * Add snippets that will be replaced into corresponding.
	 * <p>
	 * A snippet needs to be on its own line, terminated by "\n".
	 *
	 * @param key     the snippet key, without @@
	 * @param content the content to be inserted
	 * @@snippetname placeholders in the content of the description.
	 */
	public void addSnippet(String key, String content) {
		snippets.put(key, content);
	}

	/**
	 * Added one or more source snippets from test sources, available from
	 * javadoc using
	 *
	 * @param source   the class where the snippet is found
	 * @param tagNames the tag names which should be included
	 * @@tagName.
	 */
	public void addTestSourceSnippets(Class<?> source, String... tagNames) {
		for (String tagName : tagNames) {
			addSnippet(tagName, sourceSnippet(tagName, source));
		}
	}

	private static String sourceSnippet(String tagName, Class<?> source) {
		// ensure symlink is created
		Path target = new File("../docs/docs/modules/ROOT/examples/" + source.getSimpleName() + ".java").toPath().toAbsolutePath().normalize();
		if (!Files.exists(target)) {
			String sourcePath = "src/test/java/" + getPath(source);
			Path relPath = target.getParent().relativize(new File(sourcePath).toPath().toAbsolutePath());
			try {
				Files.createSymbolicLink(target, relPath);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return "[source,java,indent=0]\n" + "----\n"
				+ "include::example$" + source.getSimpleName() + ".java[tags=" + tagName + "]\n"
				+ "----\n";
	}

	public void addGithubTestSourceLink(String key, Class<?> source,
			String dir) {
		githubLink(key, source, dir, "test");
	}

	public void addGithubSourceLink(String key, Class<?> source, String dir) {
		githubLink(key, source, dir, "main");
	}

	private void githubLink(String key, Class<?> source, String dir,
			String mainOrTest) {
		String path = "https://github.com/neo4j/neo4j/blob/{neo4j-git-tag}/";
		if (dir != null) {
			path += dir + "/";
		}
		path += "src/" + mainOrTest + "/java/" + getPath(source);
		path += "[" + source.getSimpleName() + ".java]\n";
		addSnippet(key, path);
	}
}
