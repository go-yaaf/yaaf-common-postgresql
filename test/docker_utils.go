// Copyright 2022. Motty Cohen
//
// Simple utility class to start/stop docker containers using OS shell commands
//

package test

import (
	"fmt"
	"os/exec"
	"sync"
)

// region Docker configuration object ----------------------------------------------------------------------------------

// DockerContainer is used to construct docker container spec for the docker engine.
type DockerContainer struct {
	image      string            // Docker image
	name       string            // Container name
	ports      map[string]string // Container ports mapping
	vars       map[string]string // Environment variables
	labels     map[string]string // Container labels
	entryPoint []string          // Entry point
	autoRemove bool              // Automatically remove container when stopped (default: true)
}

// Name sets the container name.
func (c *DockerContainer) Name(value string) *DockerContainer {
	c.name = value
	return c
}

// Port adds a port mapping
func (c *DockerContainer) Port(external, internal string) *DockerContainer {
	c.ports[external] = internal
	return c
}

// Ports adds multiple port mappings
func (c *DockerContainer) Ports(ports map[string]string) *DockerContainer {
	for k, v := range ports {
		c.ports[k] = v
	}
	return c
}

// Var adds an environment variable
func (c *DockerContainer) Var(key, value string) *DockerContainer {
	c.vars[key] = value
	return c
}

// Vars adds multiple environment variables
func (c *DockerContainer) Vars(vars map[string]string) *DockerContainer {
	for k, v := range vars {
		c.vars[k] = v
	}
	return c
}

// Label adds custom label
func (c *DockerContainer) Label(label, value string) *DockerContainer {
	c.labels[label] = value
	return c
}

// Labels adds multiple labels
func (c *DockerContainer) Labels(label map[string]string) *DockerContainer {
	for k, v := range label {
		c.labels[k] = v
	}
	return c
}

// EntryPoint sets the entrypoint arguments of the container.
func (c *DockerContainer) EntryPoint(args ...string) *DockerContainer {
	c.entryPoint = append(c.entryPoint, args...)
	return c
}

// AutoRemove determines whether to automatically remove the container when it has stopped
func (c *DockerContainer) AutoRemove(value bool) *DockerContainer {
	c.autoRemove = value
	return c
}

// Run builds and run command
func (c *DockerContainer) Run() error {
	// construct the docker shell command
	command := "docker"
	args := make([]string, 0)
	args = append(args, "run")

	if len(c.name) > 0 {
		args = append(args, "--name")
		args = append(args, c.name)
	}

	// Expose ports
	if len(c.ports) > 0 {
		for k, v := range c.ports {
			args = append(args, "-p")
			args = append(args, fmt.Sprintf("%s:%s", k, v))
		}
	}

	// Add environment variables
	if len(c.vars) > 0 {
		for k, v := range c.vars {
			args = append(args, "-e")
			args = append(args, fmt.Sprintf("%s=%s", k, v))
		}
	}

	// Add metadata (labels)
	if len(c.labels) > 0 {
		for k, v := range c.labels {
			args = append(args, "-l")
			args = append(args, fmt.Sprintf("%s=%s", k, v))
		}
	}

	// Add docker image
	if len(c.image) > 0 {
		args = append(args, c.image)
	} else {
		return fmt.Errorf("missing image field")
	}

	// Add entry point
	if len(c.entryPoint) > 0 {
		for _, v := range c.entryPoint {
			args = append(args, v)
		}
	}

	cmd := exec.Command(command, args...)
	go cmd.Run()
	return nil
}

// Stop and kill container
func (c *DockerContainer) Stop() error {
	// construct the docker shell command
	command := "docker"
	args := make([]string, 0)
	args = append(args, "stop")

	if len(c.name) > 0 {
		args = append(args, c.name)
	} else {
		return fmt.Errorf("missing container name")
	}

	cmd := exec.Command(command, args...)
	if er := cmd.Run(); er != nil {
		return er
	}

	args2 := make([]string, 0)
	args2 = append(args2, "rm")
	args2 = append(args2, c.name)

	cmd2 := exec.Command(command, args2...)
	return cmd2.Run()
}

// endregion

// region Singleton Pattern --------------------------------------------------------------------------------------------

type dockerUtils struct {
}

var onlyOnce sync.Once
var dockerUtilsSingleton *dockerUtils = nil

// DockerUtils is a singleton pattern factory method
func DockerUtils() (du *dockerUtils) {
	onlyOnce.Do(func() {
		dockerUtilsSingleton = &dockerUtils{}
	})
	return dockerUtilsSingleton
}

// endregion

// region Docker utilities ---------------------------------------------------------------------------------------------

// CreateContainer create a docker container configuration via a fluent interface.
func (c *dockerUtils) CreateContainer(image string) *DockerContainer {
	return &DockerContainer{
		image:      image,
		ports:      make(map[string]string),
		vars:       make(map[string]string),
		labels:     make(map[string]string),
		entryPoint: make([]string, 0),
		autoRemove: true,
	}
}

// StopContainer stops and kill container
func (c *dockerUtils) StopContainer(container string) error {
	// construct the docker shell command
	command := "docker"
	args := make([]string, 0)
	args = append(args, "stop")

	if len(container) > 0 {
		args = append(args, container)
	} else {
		return fmt.Errorf("missing container name")
	}

	cmd := exec.Command(command, args...)
	if er := cmd.Run(); er != nil {
		return er
	}

	args2 := make([]string, 0)
	args2 = append(args2, "rm")
	args = append(args, container)

	cmd2 := exec.Command(command, args2...)
	return cmd2.Run()
}

// endregion
