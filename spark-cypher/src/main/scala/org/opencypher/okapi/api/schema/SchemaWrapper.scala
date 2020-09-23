package org.opencypher.okapi.api.schema

import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.types.CTRelationship


/*
// private[agens] ==> 상속 되어도 super 의 scope 제약은 유지됨
//
trait SchemaWrapper extends Schema {
	override def dropPropertiesFor(labelCombination: Set[String]): Schema
	override def forNode(knownLabels: Set[String]): Schema
	override def forRelationship(relType: CTRelationship): Schema
	override def withOverwrittenNodePropertyKeys(labelCombination: Set[String], propertyKeys: PropertyKeys): Schema
	override def withOverwrittenRelationshipPropertyKeys(relType: String, propertyKeys: PropertyKeys): Schema
}
*/


object SchemaWrapper {

	def forNode(schema: Schema, labelConstraints: Set[String]): Schema = schema.forNode(labelConstraints)

	def forRelationship(schema: Schema, relType: CTRelationship): Schema = schema.forRelationship(relType)

	def dropPropertiesFor(schema: Schema, combo: Set[String]): Schema = schema.dropPropertiesFor(combo)

	def withOverwrittenNodePropertyKeys(schema: Schema, nodeLabels: Set[String], propertyKeys: PropertyKeys): Schema = schema.withOverwrittenNodePropertyKeys(nodeLabels, propertyKeys)

	def withOverwrittenRelationshipPropertyKeys(schema: Schema, relType: String, propertyKeys: PropertyKeys): Schema = schema.withOverwrittenRelationshipPropertyKeys(relType, propertyKeys)

}
