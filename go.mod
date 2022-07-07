module github.com/mottyc/yaaf-common-postgresql

replace github.com/mottyc/yaaf-common => ./../yaaf-common

replace github.com/mottyc/yaaf-common-docker => ./../yaaf-common-docker

go 1.18

require (
	github.com/lib/pq v1.10.6
	github.com/mottyc/yaaf-common v0.0.0-00010101000000-000000000000
)

require (
	github.com/google/uuid v1.3.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
)
