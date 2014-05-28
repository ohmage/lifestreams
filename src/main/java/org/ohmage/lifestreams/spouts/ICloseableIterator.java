package org.ohmage.lifestreams.spouts;

import java.io.Closeable;
import java.util.Iterator;

public interface ICloseableIterator<E> extends Iterator<E>, Closeable {

}
