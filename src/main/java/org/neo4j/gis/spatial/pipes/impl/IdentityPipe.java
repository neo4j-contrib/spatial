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
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdentityPipe<S> extends AbstractPipe<S, S> {
    protected S processNextStart() {
        return this.starts.next();
    }
}
