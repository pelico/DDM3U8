// Package m3u8 提供 M3U8 播放列表解析功能。
// 支持 master/variant、AES-128 加密、EXT-X-MAP、byte-range。
package m3u8

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
)

// Playlist 表示一个 M3U8 播放列表
type Playlist struct {
	IsMaster      bool        // 是否为 master playlist（含 variant）
	Variants      []Variant   // master 时的多码率变体列表
	Segments      []Segment   // media playlist 时的分片列表
	Encryption    *Encryption // 加密信息（若有）
	MapURI        string      // EXT-X-MAP 初始化段 URI（fMP4 前置段）
	MapBytes      *ByteRange  // EXT-X-MAP 的 byte-range（若有）
	TargetDur     int         // EXT-X-TARGETDURATION
	Version       int         // EXT-X-VERSION
	IsLive        bool        // 是否为 live（无 #EXT-X-ENDLIST）
	KeyURICount   int         // 探测：解析过程中出现过的 #EXT-X-KEY 去重 URI 数；>1 表示源用了 key 轮换（当前实现只用最后一把 key 解全部分片）
	KeyURIsSample []string    // 探测：当 KeyURICount > 1 时，记录前 N 个 key URI 给日志展示，方便定位
}

// Variant 是 master playlist 中的一个码率变体
type Variant struct {
	URI        string
	Bandwidth  int
	Resolution string
}

// Segment 是 media playlist 中的一个分片
type Segment struct {
	Index     int
	URI       string
	Duration  float64
	ByteRange *ByteRange
}

// ByteRange 表示 byte-range 格式 (length@offset)
type ByteRange struct {
	Length int64
	Offset int64
}

// Encryption 表示 EXT-X-KEY 加密信息
type Encryption struct {
	Method string // AES-128 / NONE / SAMPLE-AES 等
	URI    string // key 的 URL
	IV     string // 初始化向量（hex）
}

// Parse 从 r 读取并解析 M3U8 内容。baseURL 用于解析相对 URI。
func Parse(r io.Reader, baseURL string) (*Playlist, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024) // 支持大行

	if !scanner.Scan() {
		return nil, fmt.Errorf("empty playlist")
	}
	if strings.TrimSpace(scanner.Text()) != "#EXTM3U" {
		return nil, fmt.Errorf("not a valid m3u8 (missing #EXTM3U)")
	}

	p := &Playlist{IsLive: true} // 默认认为是 live，遇到 ENDLIST 再置 false
	var (
		curSeg             Segment
		curSegSet          bool
		curKey             *Encryption
		segIndex           int
		curByteRange       *ByteRange
		curByteRangeHasOff bool  // 当前 EXT-X-BYTERANGE 是否显式带 @offset
		prevRangeEnd       int64 // 上一个分片 range 的 offset+length，用于缺省 offset 接续
		keyURISet          = map[string]struct{}{}
	)

	resolve := func(uri string) string {
		return resolveURI(uri, baseURL)
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			// tag 行
			tag, attrs := parseTag(line)
			switch tag {
			case "#EXT-X-VERSION":
				p.Version, _ = strconv.Atoi(attrs)
			case "#EXT-X-TARGETDURATION":
				p.TargetDur, _ = strconv.Atoi(attrs)
			case "#EXT-X-MEDIA-SEQUENCE":
				segIndex, _ = strconv.Atoi(attrs)
			case "#EXT-X-PLAYLIST-TYPE":
				// VOD / EVENT
			case "#EXT-X-ENDLIST":
				p.IsLive = false
			case "#EXT-X-STREAM-INF":
				// master variant
				v := Variant{URI: ""}
				if bw, ok := getAttr(attrs, "BANDWIDTH"); ok {
					v.Bandwidth, _ = strconv.Atoi(bw)
				}
				if res, ok := getAttr(attrs, "RESOLUTION"); ok {
					v.Resolution = res
				}
				// 下一行是 URI
				if scanner.Scan() {
					v.URI = resolve(strings.TrimSpace(scanner.Text()))
				}
				p.Variants = append(p.Variants, v)
				p.IsMaster = true
			case "#EXTINF":
				// duration, title
				parts := strings.SplitN(attrs, ",", 2)
				if len(parts) > 0 {
					curSeg.Duration, _ = strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
				}
				curSegSet = true
			case "#EXT-X-KEY":
				method, _ := getAttr(attrs, "METHOD")
				keyURI, _ := getAttr(attrs, "URI")
				iv, _ := getAttr(attrs, "IV")
				curKey = &Encryption{
					Method: method,
					URI:    resolve(unquote(keyURI)),
					IV:     iv,
				}
				p.Encryption = curKey
				// 探测 key 轮换：去重记录所有出现过的 key URI。
				// 当前实现只用最后一把 key 解全部分片，多 key 场景会解错但不报错。
				// 命中 >1 时输出 warn，让踩坑时能从日志一眼定位（无需改 Segment 结构体）。
				if keyURI != "" {
					keyURISet[resolve(unquote(keyURI))] = struct{}{}
				}
			case "#EXT-X-MAP":
				uri, _ := getAttr(attrs, "URI")
				p.MapURI = resolve(unquote(uri))
				if br, ok := getAttr(attrs, "BYTERANGE"); ok {
					p.MapBytes, _ = parseByteRange(br)
				}
			case "#EXT-X-BYTERANGE":
				curByteRange, curByteRangeHasOff = parseByteRange(attrs)
			}
			continue
		}
		// 非 # 行 = 分片 URI
		if curSegSet {
			curSeg.URI = resolve(line)
			curSeg.Index = segIndex
			segIndex++
			if curByteRange != nil {
				// 缺省 @offset：按 HLS 规范接续上一个分片 range 的末尾
				if !curByteRangeHasOff {
					curByteRange.Offset = prevRangeEnd
				}
				prevRangeEnd = curByteRange.Offset + curByteRange.Length
				curSeg.ByteRange = curByteRange
				curByteRange = nil
				curByteRangeHasOff = false
			}
			p.Segments = append(p.Segments, curSeg)
			curSeg = Segment{}
			curSegSet = false
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	// 探测 key 轮换：把去重结果填到 Playlist 上，供上层日志使用
	if n := len(keyURISet); n > 0 {
		p.KeyURICount = n
		if n > 1 {
			for u := range keyURISet {
				if len(p.KeyURIsSample) >= 3 {
					break
				}
				p.KeyURIsSample = append(p.KeyURIsSample, u)
			}
		}
	}
	return p, nil
}

// parseTag 拆分 "#TAG:attrs" 形式
func parseTag(line string) (tag, attrs string) {
	idx := strings.IndexByte(line, ':')
	if idx == -1 {
		return line, ""
	}
	return line[:idx], line[idx+1:]
}

// getAttr 从 "KEY=VALUE,KEY2=VALUE2" 提取指定 KEY
func getAttr(attrs, key string) (string, bool) {
	prefix := key + "="
	parts := splitQuoted(attrs, ',')
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if strings.HasPrefix(p, prefix) {
			return strings.TrimPrefix(p, prefix), true
		}
	}
	return "", false
}

// splitQuoted 按分隔符切分，引号内忽略分隔符
func splitQuoted(s string, sep byte) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' {
			inQuote = !inQuote
			cur.WriteByte(c)
			continue
		}
		if c == sep && !inQuote {
			out = append(out, cur.String())
			cur.Reset()
			continue
		}
		cur.WriteByte(c)
	}
	out = append(out, cur.String())
	return out
}

func unquote(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// parseByteRange 解析 HLS byte-range，返回 (range, hasOffset)。
// hasOffset=false 表示省略了 @offset，按 HLS 规范应紧接上一个 range 的末尾，
// 由解析主循环用 prevRangeEnd 填充为绝对偏移（不在此处理，因为 parser 不知道前文上下文）。
func parseByteRange(s string) (*ByteRange, bool) {
	s = strings.TrimSpace(s)
	parts := strings.SplitN(s, "@", 2)
	br := &ByteRange{}
	br.Length, _ = strconv.ParseInt(parts[0], 10, 64)
	if len(parts) > 1 {
		br.Offset, _ = strconv.ParseInt(parts[1], 10, 64)
		return br, true
	}
	return br, false
}

// resolveURI 将相对 URI 解析为绝对 URI
func resolveURI(uri, base string) string {
	if uri == "" {
		return ""
	}
	if strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://") {
		return uri
	}
	if base == "" {
		return uri
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return uri
	}
	ref, err := url.Parse(uri)
	if err != nil {
		return uri
	}
	return baseURL.ResolveReference(ref).String()
}
