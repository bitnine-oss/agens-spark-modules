package net.bitnine.agens.cypher.api.io.util

import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema

import scala.collection.mutable

import net.bitnine.agens.cypher.impl.CAPSConverters._


/**
  * Wraps a [[PropertyGraphDataSource]] and introduces a caching mechanism. First time a graph is read, it is cached in
  * Spark according to the given [[StorageLevel]] and put in the datasource internal cache. When a graph is removed from
  * the data source, it is uncached in Spark and removed from the datasource internal cache.
  *
  * @param dataSource property graph data source
  * @param storageLevel storage level for caching the graph
  */
case class CachedDataSource(
  dataSource: PropertyGraphDataSource,
  storageLevel: StorageLevel) extends PropertyGraphDataSource {

  private val cache: mutable.Map[GraphName, PropertyGraph] = mutable.Map.empty

  override def graph(name: GraphName): PropertyGraph = cache.getOrElse(name, {
    val g = dataSource.graph(name)
    g.asCaps.tables.foreach(_.persist(storageLevel))
    cache.put(name, g)
    g
  })

  override def delete(name: GraphName): Unit = cache.get(name) match {
    case Some(g) =>
      g.asCaps.tables.foreach(_.unpersist())
      cache.remove(name)
      dataSource.delete(name)

    case None =>
      dataSource.delete(name)
  }

  override def hasGraph(name: GraphName): Boolean = cache.contains(name) || dataSource.hasGraph(name)

  override def schema(name: GraphName): Option[Schema] = dataSource.schema(name)

  override def store(name: GraphName, graph: PropertyGraph): Unit = dataSource.store(name, graph)

  override def graphNames: Set[GraphName] = dataSource.graphNames
}

object CachedDataSource {

  implicit class WithCacheSupport(val ds: PropertyGraphDataSource) {

    def withCaching: CachedDataSource =
      CachedDataSource(ds, StorageLevel.MEMORY_AND_DISK)

    def withCaching(storageLevel: StorageLevel): CachedDataSource =
      CachedDataSource(ds, storageLevel)
  }

}
