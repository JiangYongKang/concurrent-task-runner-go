package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// fingerprint 计算提交内容的稳定指纹，用于“同键不同内容”的冲突判定。
// Params 使用确定性 JSON 编码参与指纹；不可 JSON 编码的入参会得到稳定回退指纹。
func fingerprint(p Payload) string {
	paramsJSON, err := json.Marshal(p.Params)
	if err != nil {
		// 回退：用类型与字符串形式，保证同内容仍得到同指纹。
		paramsJSON = []byte(fmt.Sprintf("unencodable:%T:%v", p.Params, p.Params))
	}
	sum := sha256.Sum256([]byte("v1\n" + p.Name + "\n" + string(paramsJSON)))
	return hex.EncodeToString(sum[:])
}
