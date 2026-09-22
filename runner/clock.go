package runner

import "time"

// nowFunc 可在测试中替换，以便对超时与宽限期做确定性验证。
var nowFunc = time.Now
