package cmd

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
)

var words = strings.Fields(`
acorn actor album amber anchor apple arrow atlas autumn badge baker barn
basket beach bean bell bench berry bike bird blade board boat book boots
bottle bread brick bridge brook brush bucket button cabin cable cactus cake
camel camera candle candy canoe canvas carpet carrot castle cedar chair
chalk cheese cherry chess circle clock cloud clover coast coffee comet coral
crane cream crown daisy dance delta desert desk dinner dock door dragon
dream drum duck dune eagle earth echo elbow ember engine fabric falcon farm
fern ferry field finch fire flag flame flute foam forest fox frog frost
fruit garden garlic gate giant glass globe glove goat gold grape grass
gravel guitar hammer harbor harp hawk heart hill honey horse hotel iris iron
island ivory jacket jazz jelly jewel juice jungle kayak kettle kite ladder
lake lamp laptop lemon lily lime lion lotus magnet mango maple marble meadow
melon metal mint mirror monkey moon moose motor mouse museum nest north oak
oasis ocean olive onion opal orange orbit otter owl paddle palace panda
paper parrot pasta peach pearl pebble pencil pepper piano pillow pilot pine
planet pocket pony potato puzzle quilt rabbit radio rain raven ribbon rice
river robin rocket ruby saddle salad sand scarf seal shadow shark sheep
shell silver sky snail snow soap sofa spoon spring star stone storm straw
sugar summer swan table tent tiger timber toast tomato torch tower train
tree tulip tunnel turtle valley velvet violet violin wagon walnut water
whale wheat window wizard wolf wood yacht yarn yellow zebra zinc
`)

func newCode() string {
	var b [2]byte
	_, _ = rand.Read(b[:])
	d, _ := rand.Int(rand.Reader, big.NewInt(10))
	return fmt.Sprintf("%d-%s-%s", d, words[b[0]], words[b[1]])
}

func isCode(s string) bool {
	p := strings.Split(s, "-")
	if len(p) != 3 || len(p[0]) != 1 || p[0][0] < '0' || p[0][0] > '9' {
		return false
	}
	for _, w := range p[1:] {
		if w == "" || strings.Trim(w, "abcdefghijklmnopqrstuvwxyz") != "" {
			return false
		}
	}
	return true
}
