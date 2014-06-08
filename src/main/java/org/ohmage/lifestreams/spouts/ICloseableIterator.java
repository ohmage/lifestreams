package org.ohmage.lifestreams.spouts;

import java.io.Closeable;
import java.util.Iterator;

interface ICloseableIterator<E> extends Iterator<E>, Closeable {

}
