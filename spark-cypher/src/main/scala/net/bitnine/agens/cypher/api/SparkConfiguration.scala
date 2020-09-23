package net.bitnine.agens.cypher.api

import org.opencypher.okapi.impl.configuration.ConfigOption


object SparkConfiguration {

  object MasterAddress extends ConfigOption("caps.master", "local[*]")(Some(_))

}
