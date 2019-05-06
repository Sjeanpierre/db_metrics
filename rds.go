package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"log"
	"sync"
)

var (
	awsRegions = []string{"eu-west-1", "eu-central-1", "sa-east-1", "us-east-1", "us-west-1", "us-west-2"}
)

type rDSInstances []rDSInstance

type rDSInstance struct {
	identifier string
	name       string
	tags
	*rds.DBInstance
}

type tags []tag

type tag struct {
	Name  string
	Value string
}

type ListRDSInstanceInput struct {
	filter bool
	key    string
	value  string
}

func (i rDSInstance) TagValue(lookup string) string {
	for _, tag := range i.tags {
		if lookup == tag.Name {
			return tag.Value
		}
	}
	return ""
}

func (i rDSInstance) containsTag(tagKey string) tag {
	for _, tag := range i.tags {
		if tag.Name == tagKey {
			return tag
		}
	}
	return tag{}
}

func (t tag) withValue(desiredValue string) bool {
	if t.Value == desiredValue {
		return true
	}
	return false
}

func (instances rDSInstances) FilterOnTags(tagKey string, tagValue string) rDSInstances {
	var returnSet rDSInstances
	for _, instance := range instances {
		if instance.containsTag("audit_growth").withValue("true") {
			returnSet = append(returnSet, instance)
		}
	}
	return returnSet
}

func ListRDSInstances(param ListRDSInstanceInput) rDSInstances {
	log.Println("Listing RDS instances")
	var rdsList rDSInstances
	var wg sync.WaitGroup
	var mutex = &sync.Mutex{}
	for _, region := range awsRegions {
		wg.Add(1)
		go func(reg string, x *sync.WaitGroup) {
			defer x.Done() //mark as done at the end of this function call
			sessionInfo, err := session.NewSession(&aws.Config{Region: aws.String(reg)})
			svc := rds.New(sessionInfo)
			log.Printf("fetching RDS instances in: %v\n", reg)
			params := &rds.DescribeDBInstancesInput{MaxRecords: aws.Int64(100)}
			resp, err := svc.DescribeDBInstances(params)
			if DEBUG {log.Printf("Request to %s contains next token: %v", reg, resp.Marker)}
			if err != nil {
				log.Fatalln("there was an error listing DB instances in", reg, err.Error())
			}
			var wgInner sync.WaitGroup
			var mutexInner = &sync.Mutex{}
			var tempList []rDSInstance
			for _, rdsInstance := range resp.DBInstances {
				wgInner.Add(1)
				go func(rdsI *rds.DBInstance, x1 *sync.WaitGroup) {
					defer x1.Done()
					var r rDSInstance
					r.DBInstance = rdsI
					r.identifier = *rdsI.DBInstanceArn
					r.name = *rdsI.DBInstanceIdentifier
					r.tags = rdsInstanceTags(rdsI.DBInstanceArn, svc)
					mutexInner.Lock()
					tempList = append(tempList, r)
					mutexInner.Unlock()
				}(rdsInstance, &wgInner)
			}
			wgInner.Wait()
			mutex.Lock()
			rdsList = append(rdsList, tempList...)
			mutex.Unlock()
		}(region, &wg)
	}
	wg.Wait()
	if param.filter {
		rdsList = rdsList.FilterOnTags(param.key, param.value)
	}
	return rdsList
}

func rdsInstanceTags(arn *string, svc *rds.RDS) (tags []tag) {
	params := &rds.ListTagsForResourceInput{
		ResourceName: arn,
	}
	resp, err := svc.ListTagsForResource(params)
	if err != nil {
		log.Printf("there was an error retriving Tags for %+v: %+v", arn, err.Error())
		return
	}
	for _, i := range resp.TagList {
		var t tag
		t.Name = *i.Key
		t.Value = *i.Value
		tags = append(tags, t)
	}
	return
}
