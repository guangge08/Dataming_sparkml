package utils

import org.apache.log4j.Logger

/**
  * Created by duan on 2017/6/26.
  */
trait LogSupport {
  @transient lazy val log = Logger.getLogger(this.getClass)
}
