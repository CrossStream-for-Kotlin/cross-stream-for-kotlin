package pt.isel.leic.cs4k.demo.http.pipepline

import jakarta.servlet.FilterChain
import jakarta.servlet.ServletRequest
import jakarta.servlet.ServletResponse
import jakarta.servlet.http.HttpFilter
import jakarta.servlet.http.HttpServletRequest
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.lang.management.ManagementFactory

@Component
class LoggingFilter : HttpFilter() {

    override fun doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
        val runtimeBean = ManagementFactory.getRuntimeMXBean()
        val pidHostInfo = runtimeBean.name.split("@")
        val pid = pidHostInfo[0]
        val hostName = pidHostInfo.getOrElse(1) { "Unavailable" }

        if (request is HttpServletRequest) {
            logger.info("NODE: $hostName, PID: $pid, REQUEST: ${request.requestURI}")
            request.setAttribute("node", hostName)
        }
        chain.doFilter(request, response)
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(LoggingFilter::class.java)
    }
}
