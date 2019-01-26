package main

import (
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"log"
)

type ssmClient struct {
	client *ssm.SSM
}


func newParamClient(region string) (ssmClient)  {
	sesh := session.Must(session.NewSession())
	sesh.Config.WithRegion(region)
	ssmc := ssm.New(sesh)
	return ssmClient{ssmc}
}


func (s ssmClient) getParam(path string) string {
	input := &ssm.GetParameterInput{
		Name: aws.String(path),
		WithDecryption: aws.Bool(true),
	}
	res,err := s.client.GetParameter(input)
	if err != nil {
		log.Fatal("Could not retrieve parameter set located at",path,err)
	}
	paramValue := res.Parameter.Value
	return *paramValue
}

func GetParamAtPath(path,region string) string {
	c := newParamClient(region)
	return c.getParam(path)
}
