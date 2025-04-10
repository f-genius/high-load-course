package ru.quipy.config

import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory
import org.slf4j.LoggerFactory
import org.springframework.boot.web.embedded.jetty.JettyServerCustomizer
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class AppConfigurations {

    private val logger = LoggerFactory.getLogger(AppConfigurations::class.java)

    @Bean // это для Jetty
    fun jettyServerCustomizer(): JettyServletWebServerFactory {
        val jettyServletWebServerFactory = JettyServletWebServerFactory()

        val c = JettyServerCustomizer {
            (it.connectors[0].getConnectionFactory("h2c") as HTTP2CServerConnectionFactory).maxConcurrentStreams =
                    1_000_000
        }

        jettyServletWebServerFactory.serverCustomizers.add(c)
        return jettyServletWebServerFactory
    }

}