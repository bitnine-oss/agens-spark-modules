package net.bitnine.agens.livy.util

import java.net.{URL, URLClassLoader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FsUrlStreamHandlerFactory, Path}

// https://stackoverflow.com/a/29444838
class HdfsClassLoader(classLoader: ClassLoader) extends URLClassLoader(Array.ofDim[URL](0), classLoader) {

	def addJarToClasspath(jarName: String) {
		synchronized {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory)
			var conf = new Configuration
			val fileSystem = FileSystem.get(conf)
			val path = new Path(jarName);
			if (!fileSystem.exists(path)) {
				println("File does not exists")
			}
			val uriPath = path.toUri()
			val urlPath = uriPath.toURL()
			println("** addJar to classpath: "+urlPath.getFile)
			addURL(urlPath)
		}
	}
}