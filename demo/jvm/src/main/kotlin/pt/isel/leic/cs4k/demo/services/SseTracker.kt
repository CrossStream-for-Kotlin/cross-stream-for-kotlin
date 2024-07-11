package pt.isel.leic.cs4k.demo.services

import kotlinx.coroutines.Job
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

class SseTracker(val sseEmitter: SseEmitter, var job: Job? = null)
