package peer

import "sync"

// CollectionConfig represents the configuration used to create a new peer
// collection.
type CollectionConfig struct {
	// Dependencies.
	BehaviourService   Service
	InformationService Service
}

// DefaultCollectionConfig provides a default configuration to create a new peer
// collection by best effort.
func DefaultCollectionConfig() CollectionConfig {
	var err error

	var behaviourService Service
	{
		behaviourConfig := DefaultServiceConfig()
		behaviourConfig.Kind = KindBehaviour
		behaviourService, err = NewService(behaviourConfig)
		if err != nil {
			panic(err)
		}
	}

	var informationService Service
	{
		informationConfig := DefaultServiceConfig()
		informationConfig.Kind = KindInformation
		informationService, err = NewService(informationConfig)
		if err != nil {
			panic(err)
		}
	}

	config := CollectionConfig{
		// Settings.
		BehaviourService:   behaviourService,
		InformationService: informationService,
	}

	return config
}

// NewCollection creates a new configured peer collection.
func NewCollection(config CollectionConfig) (*Collection, error) {
	// Dependencies.
	if config.BehaviourService == nil {
		return nil, maskAnyf(invalidConfigError, "behaviour service must not be empty")
	}
	if config.InformationService == nil {
		return nil, maskAnyf(invalidConfigError, "information service must not be empty")
	}

	newCollection := &Collection{
		// Dependencies.
		Behaviour:   config.BehaviourService,
		Information: config.InformationService,

		// Internals.
		bootOnce:     sync.Once{},
		shutdownOnce: sync.Once{},
	}

	return newCollection, nil
}

type Collection struct {
	// Dependencies.
	Behaviour   Service
	Information Service

	// Internals.
	bootOnce     sync.Once
	shutdownOnce sync.Once
}

func (c *Collection) Boot() {
	c.bootOnce.Do(func() {
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			c.Behaviour.Boot()
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			c.Information.Boot()
			wg.Done()
		}()

		wg.Wait()
	})
}

func (c *Collection) Shutdown() {
	c.shutdownOnce.Do(func() {
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			c.Behaviour.Shutdown()
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			c.Information.Shutdown()
			wg.Done()
		}()

		wg.Wait()
	})
}
