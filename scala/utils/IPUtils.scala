package utils

trait IPUtils {
  val IPSet=List("221.176.64", "221.176.65", "221.176.68",
    "221.176.70","221.176.71","221.176.72","221.176.73",
    "221.176.74","221.176.75","221.176.76","221.176.77","221.176.78","221.176.79")

  def isIPSet(ip:String)={
    (IpUtil.ipExistsInRange(ip, "221.176.64.00-221.176.64.255") || IpUtil.ipExistsInRange(ip, "221.176.65.00-221.176.65.255")
    || IpUtil.ipExistsInRange(ip, "221.176.68.00-221.176.68.255") || IpUtil.ipExistsInRange(ip, "221.176.70.00-221.176.79.255") || IpUtil.ipExistsInRange(ip, "221.176.69.1-221.176.69.11"))
  }
}
