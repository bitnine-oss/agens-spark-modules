package net.bitnine.agens.cypher.impl.util

import scala.annotation.StaticAnnotation
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

import net.bitnine.agens.cypher.api.io.{Labels, Node, Relationship, RelationshipType}


private[agens] object Annotation {
  def labels[E <: Node: TypeTag]: Set[String] = synchronized {
    get[Labels, E] match {
      case Some(ls) => ls.labels.toSet
      case None     => Set(runtimeClass[E].getSimpleName)
    }
  }

  def relType[E <: Relationship: TypeTag]: String = synchronized {
    get[RelationshipType, E] match {
      case Some(RelationshipType(tpe)) => tpe
      case None                        => runtimeClass[E].getSimpleName.toUpperCase
    }
  }

  def get[A <: StaticAnnotation: TypeTag, E: TypeTag]: Option[A] = synchronized {
    val maybeAnnotation = staticClass[E].annotations.find(_.tree.tpe =:= typeOf[A])
    maybeAnnotation.map { annotation =>
      val tb = typeTag[E].mirror.mkToolBox()
      val instance = tb.eval(tb.untypecheck(annotation.tree)).asInstanceOf[A]
      instance
    }
  }

  private def runtimeClass[E: TypeTag]: Class[E] = synchronized {
    val tag = typeTag[E]
    val mirror = tag.mirror
    val runtimeClass = mirror.runtimeClass(tag.tpe.typeSymbol.asClass)
    runtimeClass.asInstanceOf[Class[E]]
  }

  private def staticClass[E: TypeTag]: ClassSymbol = synchronized {
    val mirror = typeTag[E].mirror
    mirror.staticClass(runtimeClass[E].getCanonicalName)
  }
}
