/*
Copyright 2019 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	// "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog"
)

// Return true if labels1 and labels2 are the same
func sameLabels(labels1 map[string]string, labels2 map[string]string) bool {
	if labels1 == nil && labels2 == nil {
		return true
	}
	if labels1 == nil || labels2 == nil {
		return false
	}
	if len(labels1) != len(labels2) {
		return false
	}

	// both map have the same length
	for key, val1 := range labels1 {
		var val2 string
		var ok bool
		if val2, ok = labels2[key]; !ok {
			// key does not exist in labels2
			return false
		}
		if val1 != val2 {
			// value not the same
			return false
		}
	}
	// all keys in labels1 are also in labels2, and map to the same value
	return true
}

// Return true if the labels defined in matchLabels also are defined in labels
// matchLabels: match labels defined in the application
// labels: labels in the resource
// Return false if matchLabels is nil or empty
func labelsMatch(matchLabels map[string]string, labels map[string]string) bool {
	if klog.V(5) {
		klog.Infof("labelsMatch: matchLabels %s, labels: %s\n", matchLabels, labels)
	}
	if matchLabels == nil || len(matchLabels) == 0 {
		if klog.V(5) {
			klog.Infof("labelsMatch: false\n")
		}
		return false
	}
	for key, val := range matchLabels {
		otherVal, ok := labels[key]
		if !ok {
			if klog.V(5) {
				klog.Infof("labelsMatch: false\n")
			}
			return false
		}
		if strings.Compare(val, otherVal) != 0 {
			if klog.V(5) {
				klog.Infof("labelsMatch: false\n")
			}
			return false
		}
	}
	// everything match
	if klog.V(5) {
		klog.Infof("labelsMatch: true\n")
	}
	return true
}

// Return true if input kind is contained in array of groupKind
func isContainedIn(arr []groupKind, kind string) bool {
	for _, gk := range arr {
		if strings.Compare(gk.kind, kind) == 0 {
			return true
		}
	}
	return false
}

// return true if the input string is contaied in array of strings
func isContainedInStringArray(arr []string, inStr string) bool {
	for _, str := range arr {
		if strings.Compare(str, inStr) == 0 {
			return true
		}
	}
	return false
}

// Return true if labels match the given expressions
// Return false if expressions is nil or empty
func expressionsMatch(expressions []matchExpression, labels map[string]string) bool {
	if klog.V(5) {
		klog.Infof("expressionsMatch: expressions: %s len:%d, labels: %s\n", expressions, len(expressions), labels)
	}
	if expressions == nil || len(expressions) == 0 {
		if klog.V(5) {
			klog.Info("expressionsMatch: nil or empty expressions")
		}
		return false
	}
	for _, expr := range expressions {
		value, ok := labels[expr.key]
		switch expr.operator {
		case OperatorIn:
			if !ok || !isContainedInStringArray(expr.values, value) {
				// not in
				if klog.V(5) {
					klog.Infof("expressionsMatch: false\n")
				}
				return false
			}
		case OperatorNotIn:
			if !ok || isContainedInStringArray(expr.values, value) {
				// label deos notexists or there is a match
				if klog.V(5) {
					klog.Infof("expressionsMatch: false\n")
				}
				return false
			}
		case OperatorExists:
			if !ok {
				// does not exist
				if klog.V(5) {
					klog.Infof("expressionsMatch: false\n")
				}
				return false
			}
		case OperatorDoesNotExist:
			if ok {
				// exists
				if klog.V(5) {
					klog.Infof("expressionsMatch: false\n")
				}
				return false
			}
		default:
			if klog.V(5) {
				klog.Infof("expressionsMatch: false\n")
			}
			return false
		}
	}
	if klog.V(5) {
		klog.Infof("expressionsMatch: true\n")
	}
	return true
}

/* Check if resource namespace matches what application requires of its components.
Return true if resource is not namespace, or
      resource namespace matches application namespace, or
      resource namespace is in the list of application's component namespaces
*/
func resourceNamespaceMatchesApplicationComponentNamespaces(resController *ClusterWatcher, appResInfo *appResourceInfo, namespace string) bool {

	if namespace == "" {
		// resource not namespaced
		return true
	}

	if appResInfo.namespace == namespace && resController.isNamespacePermitted(namespace) {
		// same namespace
		return true
	}
	// Different namespace. Check if this Application allows this namespace
	_, ok := appResInfo.componentNamespaces[namespace]
	return ok
}

// Return true if this resource is a component of the application
func resourceComponentOfApplication(resController *ClusterWatcher, appResInfo *appResourceInfo, resInfo *resourceInfo) bool {
	if klog.V(4) {
		klog.Infof("resourceComponentOfApplication app: %s, resource: %s\n", appResInfo.name, resInfo.name)
	}

	if !resourceNamespaceMatchesApplicationComponentNamespaces(resController, appResInfo, resInfo.namespace) {
		if klog.V(4) {
			klog.Infof("    resourceComponentOfApplication false due to namespace: resource is %s/%s, application is %s/%s, component namespaces is: %s", resInfo.namespace, resInfo.name, appResInfo.namespace, appResInfo.name, appResInfo.componentNamespaces)
		}
		return false
	}

	if isSameResource(&appResInfo.resourceInfo, resInfo) {
		// self
		if klog.V(4) {
			klog.Infof("    resourceComponentOfApplication false: resource is self\n")
		}
		return false
	}
	if !isContainedIn(appResInfo.componentKinds, resInfo.kind) {
		// resource kind not what the application wants to include
		if klog.V(4) {
			klog.Infof("    resourceComponentOfApplication false: component kinds: %v, resource kind: %s\n", appResInfo.componentKinds, resInfo.kind)
		}
		return false
	}
	var hasMatchLabels = true
	if len(appResInfo.matchLabels) == 0 {
		hasMatchLabels = false
	}
	var hasMatchExpressions = true
	if len(appResInfo.matchExpressions) == 0 {
		hasMatchExpressions = false
	}

	var ret bool
	if hasMatchLabels && hasMatchExpressions {
		ret = labelsMatch(appResInfo.matchLabels, resInfo.labels) &&
			expressionsMatch(appResInfo.matchExpressions, resInfo.labels)
	} else if hasMatchLabels {
		ret = labelsMatch(appResInfo.matchLabels, resInfo.labels)
	} else if hasMatchExpressions {
		ret = expressionsMatch(appResInfo.matchExpressions, resInfo.labels)
	} else {
		ret = false
	}
	if klog.V(4) {
		klog.Infof("    resourceComponentOfApplication %t\n", ret)
	}
	return ret
}

// Delete given resource from Kube
func deleteResource(resController *ClusterWatcher, resInfo *resourceInfo) error {
	if klog.V(4) {
		klog.Infof("deleteResource GVR: %s namespace: %s name: %s\n", resInfo.gvr, resInfo.namespace, resInfo.name)
	}
	gvr, ok := resController.getWatchGVR(resInfo.gvr)
	if ok {
		// resource still being watched
		var intfNoNS = resController.plugin.dynamicClient.Resource(gvr)
		var intf dynamic.ResourceInterface
		if resInfo.namespace != "" {
			intf = intfNoNS.Namespace(resInfo.namespace)
		} else {
			intf = intfNoNS
		}

		var err error
		err = intf.Delete(resInfo.name, nil)
		if err != nil {
			if klog.V(4) {
				klog.Infof("    deleteResource error: %s %s %s %s\n", resInfo.gvr, resInfo.namespace, resInfo.name, err)
			}
			return err
		}
	}
	if klog.V(4) {
		klog.Infof("    deleteResource success: %s %s %s\n", resInfo.gvr, resInfo.namespace, resInfo.name)
	}
	return nil
}

// Check if resource is deleted
func resourceDeleted(resController *ClusterWatcher, resInfo *resourceInfo) (bool, error) {
	if klog.V(4) {
		klog.Infof("resourceDeleted  %s %s %s\n", resInfo.gvr, resInfo.namespace, resInfo.name)
	}

	gvr, ok := resController.getWatchGVR(resInfo.gvr)
	if ok {
		var intfNoNS = resController.plugin.dynamicClient.Resource(gvr)
		var intf dynamic.ResourceInterface
		if resInfo.namespace != "" {
			intf = intfNoNS.Namespace(resInfo.namespace)
		} else {
			intf = intfNoNS
		}

		// fetch the current resource
		var err error
		_, err = intf.Get(resInfo.name, metav1.GetOptions{})
		if err == nil {
			return false, fmt.Errorf("Resource %s %s %s not deleted", resInfo.gvr, resInfo.namespace, resInfo.name)
		}
		// TODO: better checking between error and resource deleted
		if klog.V(4) {
			klog.Infof("    resourceDeleted true: %s %s %s %s\n", resInfo.gvr, resInfo.namespace, resInfo.name, err)
		}
		return true, nil
	}
	return true, nil
}

// Return applications for which a resource is a direct sub-component
func getApplicationsForResource(resController *ClusterWatcher, resInfo *resourceInfo) []*appResourceInfo {
	if klog.V(4) {
		klog.Infof("getApplicationsForResource: %s\n", resInfo.name)
	}
	var ret = make([]*appResourceInfo, 0)
	// loop over all applications
	var apps = resController.listResources(coreApplicationGVR)
	for _, app := range apps {
		var unstructuredObj = app.(*unstructured.Unstructured)
		var appResInfo = &appResourceInfo{}
		if err := resController.parseAppResource(unstructuredObj, appResInfo); err == nil {
			if klog.V(4) {
				klog.Infof("    checking application: %s\n", appResInfo.name)
			}
			if resourceComponentOfApplication(resController, appResInfo, resInfo) {
				if klog.V(4) {
					klog.Infof("    found application: %s\n", appResInfo.name)
				}
				ret = append(ret, appResInfo)
			}
		} else {
			// shouldn't happen
			klog.Errorf("Unable to parse application resource %s\n", err)
		}
	}
	return ret
}

/* Recursive find all applications and ancestors for a resource
   alreadyFound: map of applications that have already been processed
*/
func findAllApplicationsForResource(resController *ClusterWatcher, obj interface{}, alreadyFound map[string]*resourceInfo) {

	var unstructuredObj = obj.(*unstructured.Unstructured)
	var resInfo = &resourceInfo{}
	resController.parseResource(unstructuredObj, resInfo)

	findAllApplicationsForResourceHelper(resController, resInfo, alreadyFound)
	return
}

func findAllApplicationsForResourceHelper(resController *ClusterWatcher, resInfo *resourceInfo, alreadyFound map[string]*resourceInfo) {

	if resInfo.gvr == coreApplicationGVR {
		key := resInfo.key()
		_, exists := alreadyFound[key]
		if exists {
			return
		}
		alreadyFound[key] = resInfo
	}

	// recursively find all parent applications
	for _, appResInfo := range getApplicationsForResource(resController, resInfo) {
		findAllApplicationsForResourceHelper(resController, &appResInfo.resourceInfo, alreadyFound)
	}
}

// Callback to handle resource changes
// TODO: DO not add resource if only kappnav status changed
var batchResourceHandler resourceActionFunc = func(resController *ClusterWatcher, rw *ResourceWatcher, eventData *eventHandlerData) error {
	key := eventData.key
	obj, exists, err := rw.store.GetByKey(key)
	applications := make(map[string]*resourceInfo)
	nonApplications := make(map[string]*resourceInfo)
	if err != nil {
		klog.Errorf("fetching key %s from store failed: %v", key, err)
		return err
	}
	if !exists {
		// delete resource
		if klog.V(3) {
			klog.Infof("    processing deleted resource %s\n", key)
		}
		// batch up all parent applications
		findAllApplicationsForResource(resController, eventData.obj, applications)
	} else {
		var resInfo = &resourceInfo{}
		resController.parseResource(eventData.obj.(*unstructured.Unstructured), resInfo)
		if eventData.funcType == UpdateFunc {
			if klog.V(3) {
				klog.Infof("    processig updated resource : %s\n", key)
			}
			var oldResInfo = &resourceInfo{}
			resController.parseResource(eventData.oldObj.(*unstructured.Unstructured), oldResInfo)
			if !sameLabels(oldResInfo.labels, resInfo.labels) {
				// label changed. Update ancestors matched by old labels
				findAllApplicationsForResource(resController, eventData.oldObj, applications)
			}
		} else {
			if klog.V(3) {
				klog.Infof("   processing added resource: %s\n", key)
			}
		}
		// find all ancestors
		findAllApplicationsForResource(resController, obj, applications)
		if resInfo.kind == DEPLOYMENT {
			resController.createActionConfigMap(resInfo)
		}
		nonApplications[resInfo.key()] = resInfo

	}
	resourceToBatch := batchResources{
		applications:    applications,
		nonApplications: nonApplications,
	}
	if klog.V(3) {
		klog.Infof("    Sending %d applications and %d resources on channel\n", len(resourceToBatch.applications), len(resourceToBatch.nonApplications))
	}
	resController.resourceChannel.send(&resourceToBatch)
	return nil
}

// if deployment.liberty && metadata.ownerReferences.kind == OpenLibertyApplication
//    create configmap
//      for each annotation
// 	   add cmd-action
// 	   add input
//        for each parm
// 		  // spec.validation.openAPIV3Schema.properties.spec.properties
// 		  //                                                .required
// 		  add field
// createActionConfigMap creates an action configmap from a componentKind's CRD
func (resController *ClusterWatcher) createActionConfigMap(resInfo *resourceInfo) {
	if klog.V(2) {
		klog.Infof("createActionConfigMap entry %v", resInfo)
	}
	tmp, ok := resInfo.metadata["ownerReferences"]
	if ok {
		if klog.V(2) {
			klog.Infof("createActionConfigMap Deployment %s has ownerReferences", resInfo.name)
		}
		ownerReferences := tmp.([]interface{})
		for _, ownerRef := range ownerReferences {
			var ownerRefMap = ownerRef.(map[string]interface{})
			kind, ok := ownerRefMap[KIND].(string)
			if ok {
				if klog.V(2) {
					klog.Infof("createActionConfigMap Deployment %s has ownerReference kind: %s", resInfo.name, kind)
				}
				if kind == "OpenLibertyApplication" {
					var objectMeta = metav1.ObjectMeta{
						Name:      "kappnav.actions.deployment-liberty." + resInfo.name,
						Namespace: resInfo.namespace,
					}
					// // Set owner of ConfigMap the same as the owner of the Deployment
					// var ownerRefs = []metav1.OwnerReference{
					// 	metav1.OwnerReference{
					// 		APIVersion:         ownerRefMap["apiVersion"].(string),
					// 		Kind:               ownerRefMap["kind"].(string),
					// 		Name:               ownerRefMap["name"].(string),
					// 		UID:                ownerRefMap["uid"].(types.UID),
					// 		Controller:         ownerRefMap["controller"].(*bool),
					// 		BlockOwnerDeletion: ownerRefMap["blockOwnerDeletion"].(*bool),
					// 	},
					// }
					// objectMeta.SetOwnerReferences(ownerRefs)
					configMap := &corev1.ConfigMap{
						ObjectMeta: objectMeta,
						// ObjectMeta: metav1.ObjectMeta{
						// 	Name:            "kappnav.actions.deployment-liberty." + resInfo.name,
						// 	Namespace:       resInfo.namespace,
						// 	OwnerReferences: ownerReferences,
						// },
						Data: map[string]string{"cmd-actions": getCmdActionsJSON(resInfo), "inputs": libertyD2opInputs},
					}
					if klog.V(2) {
						klog.Infof("createActionConfigMap configMap %v", configMap)
					}
					cfgmap, err := kubeClient.CoreV1().ConfigMaps(resInfo.namespace).Create(configMap)
					if err != nil {
						klog.Infof("createActionConfigMap Error creating action ConfigMap: %s.\n", err)
					} else if klog.V(2) {
						klog.Infof("createActionConfigMap created action ConfigMap: %v\n", cfgmap)
					}
					break
				}
			}
		}
	}
	//
	//
	//
	//
	// if action config map doesn't exist already
	// jobsClient := kubeClient.BatchV1().Jobs(getkAppNavNamespace())

	// seconds100 := int32(100)

	// job := &batchv1.Job{
	// 	ObjectMeta: metav1.ObjectMeta{
	// 		Name:      "kappnav-dynamic",
	// 		Namespace: getkAppNavNamespace(),
	// 	},
	// 	Spec: batchv1.JobSpec{
	// 		TTLSecondsAfterFinished: &seconds100,
	// 		Template: apiv1.PodTemplateSpec{
	// 			Spec: apiv1.PodSpec{
	// 				Containers: []apiv1.Container{
	// 					{
	// 						Name:            "kappnav-dynamic",
	// 						Image:           os.Getenv("KAPPNAV_INIT_IMAGE"),
	// 						Command:         []string{"/initfiles/OKDConsoleIntegration.sh"},
	// 						ImagePullPolicy: apiv1.PullPolicy(apiv1.PullAlways),
	// 						Env:             []apiv1.EnvVar{{Name: "KUBE_ENV", Value: "okd"}},
	// 					},
	// 				},
	// 				RestartPolicy: apiv1.RestartPolicyNever,
	// 			},
	// 		},
	// 	},
	// }

	// result, err := jobsClient.Create(job)
	// if err != nil {
	// 	klog.Infof("Error Creating console integration update job: %s.\n", err)
	// } else {
	// 	klog.Infof("Created console integration update job: %s.\n", result)
	// }

	// if _, err := kubeClient.CoreV1().ConfigMaps("bob").Update(configMap); err != nil {
	// 	// handle error
	// }

	// if err := kubeClient.CoreV1().ConfigMaps("bob").Delete("my-configmap", &metav1.DeleteOptions{}); err != nil {
	// 	// handle error
	// }
}

func getCmdActionsJSON(resInfo *resourceInfo) string {

	return "    [\n" +
		"      {\n" +
		"        \"name\": \"getLibertyDump\",\n" +
		"        \"text\": \"Get Liberty Dump\",\n" +
		"        \"description\": \"Get Liberty dump.\",\n" +
		"        \"image\": \"docker.io/pwbennet/app-nav-cmds:latest\",\n" +
		"        \"cmd-pattern\": \"sh liberty-d2ops.sh dump ${input.dump-pod-name} " + resInfo.namespace + " ${input.dump-type}\",\n" +
		"        \"requires-input\": \"liberty-dump-parms\"\n" +
		"      },\n" +
		"      {\n" +
		"        \"name\": \"getLibertyTrace\",\n" +
		"        \"text\": \"Get Liberty Trace\",\n" +
		"        \"description\": \"Get Liberty trace.\",\n" +
		"        \"image\": \"docker.io/pwbennet/app-nav-cmds:latest\",\n" +
		"        \"cmd-pattern\": \"sh liberty-d2ops.sh trace ${input.trace-pod-name} " + resInfo.namespace + " ${input.trace-spec} ${input.trace-max-file-size} ${input.trace-max-files} ${input.trace-disable}\",\n" +
		"        \"requires-input\": \"liberty-trace-parms\"\n" +
		"      }\n" +
		"    ]"
}

var libertyD2opInputs = "    {\n" +
	"      \"liberty-dump-parms\": {\n" +
	"          \"title\": \"Liberty Dump Parameters\",\n" +
	"          \"fields\": {\n" +
	"              \"dump-pod-name\":\n" +
	"                  { \"label\": \"Pod Name\", \"type\" : \"string\", \"size\":\"large\", \"description\": \"Name of Liberty pod\", \"default\": \"\", \"optional\":false },\n" +
	"              \"dump-type\":\n" +
	"                  { \"label\": \"Dump Type: heap, thread, system\", \"type\" : \"list\", \"size\": \"medium\", \"values\": [ \"heap\", \"system\", \"thread\" ], \"description\": \"Type of Dump\", \"default\": \"heap\", \"optional\":false }\n" +
	"          }\n" +
	"      },\n" +
	"      \"liberty-trace-parms\": {\n" +
	"          \"title\": \"Liberty Trace Parameters\",\n" +
	"          \"fields\": {\n" +
	"              \"trace-pod-name\":\n" +
	"                  { \"label\": \"Pod Name\", \"type\" : \"string\", \"size\":\"large\", \"description\": \"Name of Liberty pod\", \"default\": \"\", \"optional\":false },\n" +
	"              \"trace-spec\":\n" +
	"                  { \"label\": \"Trace Specification\", \"type\" : \"string\", \"size\":\"large\", \"description\": \"Trace Specification\", \"default\": \"*=info\", \"optional\":true },\n" +
	"              \"trace-max-file-size\":\n" +
	"                  { \"label\": \"Maximum trace file size in megabytes\", \"type\" : \"string\", \"description\": \"Maximum trace file size in megabytes\", \"default\": \"\", \"optional\":true },\n" +
	"              \"trace-max-files\":\n" +
	"                  { \"label\": \"Maximum number of trace files\", \"type\" : \"string\", \"size\":\"large\", \"description\": \"Maximum number of trace files\", \"default\": \"\", \"optional\":true },\n" +
	"              \"trace-disable\":\n" +
	"                  { \"label\": \"Disable Trace\", \"type\" : \"string\", \"size\":\"large\", \"description\": \"Disable trace\", \"default\": \"false\", \"optional\":true }\n" +
	"          }\n" +
	"      }\n" +
	"    }"

// Start watching component kinds of the application. Also put
// application on batch of applications to recalculate status
func startWatchApplicationComponentKinds(resController *ClusterWatcher, obj interface{}, applications map[string]*resourceInfo) error {
	if klog.V(4) {
		klog.Infof("startWatchApplicationComponentKinds: %T %s\n", obj, obj)
	}
	switch obj.(type) {
	case *unstructured.Unstructured:
		var unstructuredObj = obj.(*unstructured.
			Unstructured)

		var appInfo = &appResourceInfo{}
		if err := resController.parseAppResource(unstructuredObj, appInfo); err == nil {
			// start watching all component kinds of the application
			var componentKinds = appInfo.componentKinds
			nsFilter := resController.nsFilter
			for _, elem := range componentKinds {
				// TODO: PWB process group here, map to gvr
				/* Start processing kinds in the application's namespace */
				nsFilter.permitNamespace(resController, elem.gvr, appInfo.resourceInfo.namespace)

				/* also permit namespaces in the kappnav.component.namespaces annotation */
				for _, ns := range appInfo.componentNamespaces {
					nsFilter.permitNamespace(resController, elem.gvr, ns)
				}

				err := resController.AddToWatch(elem.gvr)
				if err != nil {
					// TODO: should we continue to process the rest of kinds?
					return err
				}
			}
			applications[appInfo.resourceInfo.key()] = &appInfo.resourceInfo
		}

		return nil

	default:
		return fmt.Errorf("    batchAddModifyApplication.addApplication: not Unstructured: type: %T val: % s", obj, obj)
	}
}

// Handle application changes
// TODO: Do not add applications to be processed if only kappnav status changed
var batchApplicationHandler resourceActionFunc = func(resController *ClusterWatcher, rw *ResourceWatcher, eventData *eventHandlerData) error {
	if klog.V(4) {
		klog.Infof("batchApplicationHander\n")
	}

	key := eventData.key
	obj, exists, err := rw.store.GetByKey(key)
	if err != nil {
		klog.Errorf("   batchApplicationhandler fetching key %s failed: %v", key, err)
		return err
	}
	applications := make(map[string]*resourceInfo)
	nonApplications := make(map[string]*resourceInfo)
	if !exists {
		// application is gone. Update parent applications
		if klog.V(3) {
			klog.Infof("    processing application deleted: %s\n", key)
		}
		// batch up all ancestor applications
		findAllApplicationsForResource(resController, eventData.obj, applications)
	} else {
		if eventData.funcType == UpdateFunc {
			// application updated
			if klog.V(3) {
				klog.Infof("    processing application updated: %s\n", key)
			}
			var oldResInfo = &resourceInfo{}
			resController.parseResource(eventData.oldObj.(*unstructured.Unstructured), oldResInfo)
			var newResInfo = &resourceInfo{}
			resController.parseResource(eventData.obj.(*unstructured.Unstructured), newResInfo)
			// Something changed. batch up ancestors of application
			// TODO: optimize by finding ancestors only if label or
			// selector changed.  Note that a label change affects which
			// parent applications selects this application. A selector
			// changes affects which sub-components are included in calculation
			findAllApplicationsForResource(resController, eventData.oldObj, applications)
		} else {
			if klog.V(3) {
				klog.Infof("    processing application added: %s\n", key)
			}
		}
		err = startWatchApplicationComponentKinds(resController, obj, applications)
		if err != nil {
			klog.Errorf("    process application error %s\n", err)
			return err
		}
		findAllApplicationsForResource(resController, eventData.obj, applications)
	}
	resourceToBatch := batchResources{
		applications:    applications,
		nonApplications: nonApplications,
	}
	if klog.V(3) {
		klog.Infof("    Sending %d applications and %d resources on channel\n", len(resourceToBatch.applications), len(resourceToBatch.nonApplications))
	}
	resController.resourceChannel.send(&resourceToBatch)

	return nil
}
