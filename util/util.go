package util

import "log"

func CheckGoPanic() {
	if r := recover(); r != nil {
		log.Println("panic recovered", r)
	}
}
