package org.neo4j.gis.spatial.pipes.impl;

/**
 * The IdentityPipe is the most basic pipe.
 * It simply maps the input to the output without any processing.
 * <p/>
 * <pre>
 * protected S processNextStart() {
 *  return this.starts.next();
 * }
 * </pre>
 * <p/>
 *
 * @author <a href="http://markorodriguez.com" >Marko A. Rodriguez</a>
 */
public class IdentityPipe<S> extends AbstractPipe<S, S> {

	@Override
	protected S processNextStart() {
		return this.starts.next();
	}
}
