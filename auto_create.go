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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog"
)

const (

	// AppAutoCreate ...
	AppAutoCreate = "kappnav.app.auto-create"
	// AppAutoCreateName ...
	AppAutoCreateName = "kappnav.app.auto-create.name"
	// AppAutoCreateKinds ...
	AppAutoCreateKinds = "kappnav.app.auto-create.kinds"
	// AppAutoCreateVersion ...
	AppAutoCreateVersion = "kappnav.app.auto-create.version"
	// AppAutoCreateLabel ...
	AppAutoCreateLabel = "kappnav.app.auto-create.label"
	// AppAutoCreateLabelValues ...
	AppAutoCreateLabelValues = "kappnav.app.auto-create.labels-values"
	// AppAutoCreated ...
	AppAutoCreated = "kappnav.app.auto-created"
	// AppAutoCreatedFromName ...
	AppAutoCreatedFromName = "kappnav.app.auto-created.from.name"
	// AppAutoCreatedFromKind ...
	AppAutoCreatedFromKind = "kappnav.app.auto-created.from.kind"

	/* labels for auto-created app */
	labelName       = "app.kubernetes.io/name"
	labelVersion    = "app.kubernetes.io/version"
	labelAutoCreate = "kappnav.app.auto-created"

	defaultAutoCreateAppVersion = "1.0.0"
	defaultAutoCreateAppLabel   = "app"
)

/* default kinds for auto-created app*/
var defaultKinds = []groupKind{
	groupKind{group: "apps", kind: "Deployment"},
	groupKind{group: "apps", kind: "StatefulSet"},
	groupKind{group: "", kind: "Service"},
	groupKind{group: "extensions", kind: "Ingress"},
	groupKind{group: "", kind: "ConfigMap"}}

/* information about resource from which to auto-create applications */
type autoCreateResourceInfo struct {
	resourceInfo
	autoCreate            bool
	autoCreateName        string
	autoCreateKinds       []groupKind
	autoCreateVersion     string
	autoCreateLabel       string
	autoCreateLabelValues []string
}

/* Parse a comma separated string into array of strings. Leading and trailing spaces are removed
   Examples:
       instr: " A , B, C D  "
       output:  []string { "A", "B", "C D" }
*/
func stringToArrayOfString(inStr string) []string {
	tmpArray := strings.Split(inStr, ",")
	var finalArray = make([]string, 0)
	for _, str := range tmpArray {
		str = strings.TrimLeft(str, " ")
		str = strings.TrimRight(str, " ")
		if len(str) != 0 {
			finalArray = append(finalArray, str)
		}
	}
	return finalArray
}

func testNonAlphaNumeric(r rune) bool {
	if (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9') {
		return false
	}
	return true
}

func isStringAlphaNumeric(str string) bool {
	for _, r := range str {
		if testNonAlphaNumeric(r) {
			return false
		}
	}
	return true
}

/* Trim leading and trailing non-alphanumeric from string.
   Return new string if the remainder is alphnumeric. and not empty
    ok == true if a valid string can be returned.
*/
func trimNonAlphaNumeric(str string) (string, bool) {
	newStr := strings.TrimFunc(str, testNonAlphaNumeric)
	if newStr == "" {
		return "", false
	}
	if isStringAlphaNumeric(newStr) {
		return newStr, true
	}
	return "", false
}

/* Convert a comma separate string into array of alphanumeric
First, the string is split by ","
Then all trailing and ending non-alpha numeric are removed
Any string that is not all alpha-numeric is discarded.
An array is constructed of remaining strings.
*/
func stringToArrayOfAlphaNumeric(inStr string) []string {
	tmpArray := stringToArrayOfString(inStr)
	var ret = make([]string, 0)
	for _, val := range tmpArray {
		if newval, ok := trimNonAlphaNumeric(val); ok {
			ret = append(ret, newval)
		}
	}
	return ret
}

/* Convert a comma separate string into map of alphanumeric
First, the string is split by ","
Then all trailing and ending non-alpha numeric are removed
Any string that is not all alpha-numeric is discarded.
An map is constructed of remaining strings, where the key/value are the same
*/
func stringToNamespaceMap(inStr string) map[string]string {
	tmpArray := stringToArrayOfString(inStr)
	ret := make(map[string]string)
	for _, val := range tmpArray {
		if newval, ok := trimNonAlphaNumeric(val); ok {
			// Note: Should we issue some msg here if a namespace
			// contains invalid chars? Do we really want to just
			// use the trimmed name?
			ret[newval] = newval
		}
	}
	return ret
}

func stringToArrayOfLabelValues(inStr string) []string {
	tmpArray := stringToArrayOfString(inStr)
	for index, val := range tmpArray {
		tmpArray[index] = toLabelName(val)
	}
	return tmpArray
}

/* Parse a resource from which to auto create applications
   unstructuredObj: object to be parsed
   Return an autoCreateResourceInfo  to represent the resource
        Return  nil if the label kappnav.app.auto-create is not set to true.
       Note: If parsing error occurs for annotations, default values are used.
*/
func (resController *ClusterWatcher) parseAutoCreateResourceInfo(unstructuredObj *unstructured.Unstructured) *autoCreateResourceInfo {

	resourceInfo := &autoCreateResourceInfo{}
	resController.parseResource(unstructuredObj, &resourceInfo.resourceInfo)

	autoCreate, ok := resourceInfo.resourceInfo.labels[AppAutoCreate]
	if !ok {
		return nil
	}

	if autoCreate != "true" {
		return nil
	}

	annotationsObj, ok := resourceInfo.metadata[ANNOTATIONS]
	var annotations map[string]interface{}
	if ok {
		annotations = annotationsObj.(map[string]interface{})
	} else {
		annotations = make(map[string]interface{})
	}

	tmp, ok := annotations[AppAutoCreateName]
	if !ok {
		// use default.
		// TODO: Transform it due to syntax restriction
		resourceInfo.autoCreateName = resourceInfo.name
	} else {
		resourceInfo.autoCreateName, ok = tmp.(string)
		if !ok {
			klog.Errorf("Metadata %s for resource %s/%s is not a string. Using default.", AppAutoCreateName, resourceInfo.resourceInfo.namespace, resourceInfo.resourceInfo.name)
			resourceInfo.autoCreateName = resourceInfo.name
		} else {
			resourceInfo.autoCreateName = toDomainName(resourceInfo.autoCreateName)
		}
	}

	tmp, ok = annotations[AppAutoCreateKinds]
	if !ok {
		resourceInfo.autoCreateKinds = defaultKinds
	} else {
		autoCreateKindsStr, ok := tmp.(string)
		if !ok {
			klog.Errorf("Metadata %s for resource %s/%s is not a string of comma separated kinds. Using default.", AppAutoCreateKinds, resourceInfo.resourceInfo.namespace, resourceInfo.resourceInfo.name)
			resourceInfo.autoCreateKinds = defaultKinds
		} else {
			arrayOfString := stringToArrayOfAlphaNumeric(autoCreateKindsStr)
			if len(arrayOfString) == 0 {
				klog.Errorf("Metadata %s for resource %s/%s does not contain comma separated kinds. Its current value is %s. Switching to default.", AppAutoCreateKinds, resourceInfo.resourceInfo.namespace, resourceInfo.resourceInfo.name, autoCreateKindsStr)
				resourceInfo.autoCreateKinds = defaultKinds
			} else {
				/* Transform array of string to array of groupKind */
				resourceInfo.autoCreateKinds = make([]groupKind, 0, len(arrayOfString))
				for _, val := range arrayOfString {
					var group string
					var kind string
					var gvr schema.GroupVersionResource
					// a / delimiter indicates the kappnav.app.auto-create.kinds anno entry is group/kind
					split := strings.Split(val, "/")
					if len(split) > 1 {
						group = split[0]
						kind = split[1]
						if klog.V(4) {
							klog.Infof("parseAutoCreateResourceInfo using group: %s kind: %s from kappnav.app.auto-create.kinds entry: %s", group, kind, val)
						}
						gvr, ok = resController.getWatchGVRForKind(kind)
						if !ok {
							gvr, ok = resController.getWatchGVRForKind(kind)
						}
					} else {
						// no /, kappnav.app.auto-create.kinds anno entry is just a kind
						kind = val
						gvr, ok = resController.getWatchGVRForKind(kind)
						if ok {
							group = gvr.Group
							if klog.V(4) {
								klog.Infof("parseAutoCreateResourceInfo using group: %s from watch GVR for kind: %s", group, kind)
							}
						} else {
							gvr, ok = coreKindToGVR[val]
							if ok {
								group = gvr.Group
								if klog.V(4) {
									klog.Infof("parseAutoCreateResourceInfo using group: #s from default GVR for core kind: %s, using default group: App", val)
								}
							} else {
								group = "apps"
								if klog.V(4) {
									klog.Infof("parseAutoCreateResourceInfo no GVR found for kind: %s, using default group: apps", val)
								}
							}
						}
					}
					if (gvr != schema.GroupVersionResource{}) {
						resourceInfo.autoCreateKinds = append(resourceInfo.autoCreateKinds, groupKind{group, val, gvr})
					} else {
						if klog.V(4) {
							klog.Infof("parseAutoCreateResourceInfo no GVR found for kappnav.app.auto-create.kinds entry: %s, skipping", val)
						}
					}
				}
			}
		}
	}

	tmp, ok = annotations[AppAutoCreateVersion]
	if !ok {
		resourceInfo.autoCreateVersion = defaultAutoCreateAppVersion
	} else {
		resourceInfo.autoCreateVersion, ok = tmp.(string)
		if !ok {
			klog.Errorf("Metadata %s for resources %s/%s is not a string. Using default.", AppAutoCreateVersion, resourceInfo.resourceInfo.namespace, resourceInfo.resourceInfo.name)
			resourceInfo.autoCreateVersion = defaultAutoCreateAppVersion
		}
	}

	tmp, ok = annotations[AppAutoCreateLabel]
	if !ok {
		resourceInfo.autoCreateLabel = defaultAutoCreateAppLabel
	} else {
		resourceInfo.autoCreateLabel, ok = tmp.(string)
		if !ok {
			klog.Errorf("Metadata %s for resoruce %s/%s is not a string. Using default.", AppAutoCreateLabel, resourceInfo.resourceInfo.namespace, resourceInfo.resourceInfo.name)
			resourceInfo.autoCreateLabel = defaultAutoCreateAppLabel
		} else {
			resourceInfo.autoCreateLabel = toLabel(resourceInfo.autoCreateLabel)
		}
	}

	tmp, ok = annotations[AppAutoCreateLabelValues]
	if !ok {
		resourceInfo.autoCreateLabelValues = []string{resourceInfo.autoCreateName}
	} else {
		autoCreateValuesStr, ok := tmp.(string)
		if !ok {
			klog.Errorf("Metadata %s for resource %s/%s does not contain comma separted label values. Using default.", AppAutoCreateLabelValues, resourceInfo.resourceInfo.namespace, resourceInfo.resourceInfo.name)
			resourceInfo.autoCreateLabelValues = []string{resourceInfo.autoCreateName}
		} else {
			labelValues := stringToArrayOfLabelValues(autoCreateValuesStr)
			if len(labelValues) == 0 {
				klog.Errorf("Metadata %s for resource %s/%s does not contain comma separated label values. Its current value: %s.  Using default.", AppAutoCreateLabelValues, resourceInfo.resourceInfo.namespace, resourceInfo.resourceInfo.name, autoCreateValuesStr)
				resourceInfo.autoCreateLabelValues = []string{resourceInfo.autoCreateName}
			} else {
				resourceInfo.autoCreateLabelValues = labelValues
			}
		}
	}
	return resourceInfo
}

/*
 * Handle auto creation of application from resources
 */
var autoCreateAppHandler resourceActionFunc = func(resController *ClusterWatcher, rw *ResourceWatcher, eventData *eventHandlerData) error {
	key := eventData.key
	_, exists, err := rw.store.GetByKey(key)
	if err != nil {
		klog.Errorf("fetching key %s from store failed: %v", key, err)
		return err
	}

	if klog.V(5) {
		klog.Infof("autoCreateAppHandler Processing %d current object: %s", eventData.funcType, eventData.obj.(*unstructured.Unstructured))
		if eventData.funcType == UpdateFunc {
			klog.Infof("autoCreateAppHandler Processing %d old object : %s", eventData.funcType, eventData.oldObj.(*unstructured.Unstructured))
		}
	}
	resourceInfo := resController.parseAutoCreateResourceInfo(eventData.obj.(*unstructured.Unstructured))
	if resourceInfo == nil {
		if klog.V(5) {
			klog.Infof("autoCreateAppHandler: not a resource to auto-create")
		}
		// not an auto-create resource
		if eventData.funcType == UpdateFunc {
			oldResInfo := resController.parseAutoCreateResourceInfo(eventData.oldObj.(*unstructured.Unstructured))
			if oldResInfo != nil {
				/* Went from auto-created to not auto-created */
				if klog.V(2) {
					klog.Infof("Applications no longer auto-created for resource %s, kind: %s. Deleting existing auto-created application", key, oldResInfo.kind)
				}
				deleteAutoCreatedApplicationsForResource(resController, rw, oldResInfo)
			}
		}
		return nil
	}
	if !exists {
		// resource deleted
		if klog.V(3) {
			klog.Infof("    processing deleted resource %s", key)
		}
		// Delete all auto-created applications
		deleteAutoCreatedApplicationsForResource(resController, rw, resourceInfo)
	} else if eventData.funcType == UpdateFunc || eventData.funcType == AddFunc {
		if klog.V(3) {
			klog.Infof("processing add/update resource : %s, kind: %s", key, resourceInfo.kind)
		}

		if eventData.funcType == UpdateFunc {
			oldResInfo := resController.parseAutoCreateResourceInfo(eventData.oldObj.(*unstructured.Unstructured))
			if oldResInfo != nil {
				if oldResInfo.autoCreateName != resourceInfo.autoCreateName {
					if klog.V(2) {
						klog.Infof("Auto-created application for resource kind %s, name %s changed from %s to %s. Deleting existing auto-created application", resourceInfo.kind, key, oldResInfo.autoCreateName, resourceInfo.autoCreateName)
					}
					// name of auto created resource changed. Deleted the old
					deleteAutoCreatedApplicationsForResource(resController, rw, oldResInfo)
				}
			}
		}
		err := autoCreateModifyApplication(resController, rw, resourceInfo)
		if err != nil {
			klog.Errorf("Error processing processig add/update resource %s: %s", key, err)
			return err
		}
	} else {
		if klog.V(5) {
			klog.Infof("autoCreateAppHandler skipping event")
		}
	}
	return nil
}

/* Get the auto create params from an auto-created application */
func getAutoCreatedFromParams(annotations map[string]interface{}) (string, string, bool) {
	nameObj, ok := annotations[AppAutoCreatedFromName]
	if !ok {
		return "", "", false
	}
	name, ok := nameObj.(string)
	if !ok {
		return "", "", false
	}

	kindObj, ok := annotations[AppAutoCreatedFromKind]
	if !ok {
		return "", "", false
	}
	kind, ok := kindObj.(string)
	if !ok {
		return "", "", false
	}
	return name, kind, true
}

func applicationAutoCreatedFromResource(appResource *appResourceInfo, resInfo *autoCreateResourceInfo) bool {
	annotations, ok := appResource.metadata[ANNOTATIONS]
	if !ok {
		if klog.V(5) {
			klog.Infof("applicationAutoCreatedFromResource: missing annotations")
		}
		return false
	}
	fromName, fromKind, ok := getAutoCreatedFromParams(annotations.(map[string]interface{}))
	if !ok {
		if klog.V(5) {
			klog.Infof("applicationAutoCreatedFromResource: missing created-from name or kind")
		}
		return false
	}
	if klog.V(5) {
		klog.Infof("applicationAutoCreatedFromResource: app namesapce: %s, created-from name: %s, kind: %s; resource namespace: %s, name: %s, kind: %s", appResource.namespace, fromName, fromKind, resInfo.namespace, resInfo.name, resInfo.kind)
	}
	if appResource.resourceInfo.namespace == resInfo.resourceInfo.namespace && fromKind == resInfo.kind && fromName == resInfo.name {
		return true
	}
	return false
}

func deleteAutoCreatedApplicationsForResource(resController *ClusterWatcher, rw *ResourceWatcher, resInfo *autoCreateResourceInfo) error {
	if klog.V(4) {
		klog.Infof("Deleting auto-created application %s/%s", resInfo.namespace, resInfo.autoCreateName)
	}

	gvr, ok := resController.getWatchGVR(coreApplicationGVR)
	if ok {
		var intfNoNS = resController.plugin.dynamicClient.Resource(gvr)
		var intf dynamic.ResourceInterface
		if resInfo.namespace != "" {
			intf = intfNoNS.Namespace(resInfo.namespace)
		} else {
			intf = intfNoNS
		}

		// fetch the current resource
		var unstructuredObj *unstructured.Unstructured
		var err error
		unstructuredObj, err = intf.Get(resInfo.autoCreateName, metav1.GetOptions{})
		if err != nil {
			/* Most likely resource does not exist */
			klog.Infof("Error getting application to be deleted %s/%s. Error:%s", resInfo.namespace, resInfo.autoCreateName, err)
			return nil
		}

		var appResInfo = &appResourceInfo{}
		resController.parseAppResource(unstructuredObj, appResInfo)
		if applicationAutoCreatedFromResource(appResInfo, resInfo) {
			if klog.V(4) {
				klog.Infof("    deleting application: %s/%s", appResInfo.namespace, appResInfo.name)
			}
			err := deleteResource(resController, &appResInfo.resourceInfo)
			if err != nil {
				klog.Errorf("Error deleting application: %s %s %s. Error: %s", resInfo.kind, resInfo.namespace, resInfo.name, err)
			} else {
				if klog.V(2) {
					klog.Infof("Deleted auto-created application %s/%s", appResInfo.namespace, appResInfo.name)
				}
			}
			return err
		}
		if klog.V(4) {
			klog.Infof("    application %s/%s not auto-created", appResInfo.namespace, appResInfo.name)
		}
	}

	return nil
}

func autoCreateModifyApplication(resController *ClusterWatcher, rw *ResourceWatcher, resInfo *autoCreateResourceInfo) error {
	if klog.V(4) {
		klog.Infof("autoCreateModifyApplication from %s/%s", resInfo.resourceInfo.namespace, resInfo.resourceInfo.name)
	}
	gvr, ok := resController.getWatchGVR(coreApplicationGVR)
	if ok {
		var intfNoNS = resController.plugin.dynamicClient.Resource(gvr)
		var intf dynamic.ResourceInterface
		if resInfo.namespace != "" {
			intf = intfNoNS.Namespace(resInfo.namespace)
		} else {
			intf = intfNoNS
		}

		// fetch the current resource
		var unstructuredObj *unstructured.Unstructured
		var err error
		unstructuredObj, err = intf.Get(resInfo.autoCreateName, metav1.GetOptions{})
		if err != nil {
			/* TODO: check error. Most likely resource does not exist */
			err = createApplication(resController, rw, resInfo)
			return err
		}

		/* If we get here, modify the  applications */
		if klog.V(5) {
			klog.Infof("autoCreateModifyApplication: checking existing application %s", unstructuredObj)
		}
		appResInfo := &appResourceInfo{}
		err = resController.parseAppResource(unstructuredObj, appResInfo)
		if err != nil {
			klog.Errorf("parseApplication error %s", err)
			return err
		}
		if !autoGenerated(appResInfo) {
			autoGenErr := fmt.Errorf("Unable to update application %s/%s as it was not auto-generated", resInfo.namespace, resInfo.name)
			klog.Error(autoGenErr)
			return autoGenErr
		}

		if autoCreatedApplicationNeedsUpdate(appResInfo, resInfo) {
			// change status
			if klog.V(2) {
				klog.Infof("Changing autocreated application annotations and labels for %s/%s", resInfo.namespace, resInfo.autoCreateName)
			}
			setApplicationAnnotationLabels(unstructuredObj, resInfo)
			_, err = intf.Update(unstructuredObj, metav1.UpdateOptions{})
			if err != nil {
				if klog.V(2) {
					klog.Errorf("    error modifying application%s", err)
				}
			} else {
				if klog.V(2) {
					klog.Infof("Updated auto-created application %s/%s", resInfo.namespace, resInfo.autoCreateName)
				}
			}
			return err
		}
		if klog.V(5) {
			klog.Infof("Application does not need update")
		}
		return nil
	}
	return fmt.Errorf("Unable to find GVR for kind Application")
}

var autoCreatedAppJSONTemplate = "{ {{__NEWLINE__}}" +
	"    \"apiVersion\": \"app.k8s.io/v1beta1\", {{__NEWLINE__}}" +
	"    \"kind\": \"Application\", {{__NEWLINE__}}" +
	"    \"metadata\": { {{__NEWLINE__}}" +
	"        \"annotations\": { {{__NEWLINE__}}" +
	"               \"kappnav.app.auto-created.from.name\" : \"{{__CREATED_FROM_NAME__}}\",{{__NEWLINE__}}" +
	"               \"kappnav.app.auto-created.from.kind\" : \"{{__CREATED_FROM_KIND__}}\"{{__NEWLINE__}}" +
	"        }, {{__NEWLINE__}}" +
	"        \"labels\": {{{__NEWLINE__}}" +
	"             \"kappnav.app.auto-created\" : \"true\",{{__NEWLINE__}}" +
	"             \"app.kubernetes.io/name\": \"{{__APP_NAME__}}\", {{__NEWLINE__}}" +
	"             \"app.kubernetes.io/version\" :  \"{{__APP_VERSION__}}\", {{__NEWLINE__}}" +
	"            \"{{__APP_LABEL__}}\": \"{{__APP_NAME__}}\" {{__NEWLINE__}}" +
	"        },{{__NEWLINE__}}" +
	"        \"name\": \"{{__APP_NAME__}}\",{{__NEWLINE__}}" +
	"        \"namespace\": \"{{__NAMESPACE__}}\"{{__NEWLINE__}}" +
	"    },{{__NEWLINE__}}" +
	"    \"spec\": { {{__NEWLINE__}}" +
	"        \"componentKinds\": [ {{__COMPONENT_KINDS__}} ], {{__NEWLINE__}}" +
	"        \"selector\": { {{__SELECTOR_VALUE__}} } {{__NEWLINE__}}" +
	"        }{{__NEWLINE__}}" +
	"    }{{__NEWLINE__}}" +
	"}{{__NEWLINE__}}"

var autoCreateMatchLabel = "\"matchLabels\": { \"{{__APP_LABEL__}}\": \"{{__LABEL_VALUE__}}\"  } "

var autoCreateMatchExpression = "\"matchExpressions\": [ { \"key\": \"{{__APP_LABEL__}}\",  \"operator\": \"In\", \"values\": [{{__MATCH_VALUES__}}] } ]}"

func getApplicationJSON(resInfo *autoCreateResourceInfo) string {
	var template = strings.Replace(autoCreatedAppJSONTemplate, "{{__NEWLINE__}}", "\n", -1)
	template = strings.Replace(template, "{{__CREATED_FROM_NAME__}}", resInfo.name, -1)
	template = strings.Replace(template, "{{__CREATED_FROM_KIND__}}", resInfo.kind, -1)
	template = strings.Replace(template, "{{__APP_NAME__}}", resInfo.autoCreateName, -1)
	template = strings.Replace(template, "{{__APP_LABEL__}}", resInfo.autoCreateLabel, -1)
	template = strings.Replace(template, "{{__NAMESPACE__}}", resInfo.namespace, -1)
	template = strings.Replace(template, "{{__APP_VERSION__}}", resInfo.autoCreateVersion, -1)

	componentKindsStr := ""
	for index, componentKind := range resInfo.autoCreateKinds {
		componentKindsStr += "{ \"group\" : \"" + componentKind.group + "\", \"kind\" : \"" + componentKind.kind + "\" }"
		if index < len(resInfo.autoCreateKinds)-1 {
			componentKindsStr += ", "
		}
	}
	template = strings.Replace(template, "{{__COMPONENT_KINDS__}}", componentKindsStr, -1)

	var selectorValue string
	if len(resInfo.autoCreateLabelValues) == 1 {
		selectorValue = strings.Replace(autoCreateMatchLabel, "{{__NEWLINE__}}", "\n", -1)
		selectorValue = strings.Replace(selectorValue, "{{__NEWLINE__}}", "\n", -1)
		selectorValue = strings.Replace(selectorValue, "{{__APP_LABEL__}}", resInfo.autoCreateLabel, -1)
		selectorValue = strings.Replace(selectorValue, "{{__LABEL_VALUE__}}", resInfo.autoCreateLabelValues[0], -1)
	} else {
		selectorValue = strings.Replace(autoCreateMatchExpression, "{{__NEWLINE__}}", "\n", -1)
		selectorValue = strings.Replace(selectorValue, "{{__APP_LABEL__}}", resInfo.autoCreateLabel, -1)
		matchValuesStr := ""
		for index, value := range resInfo.autoCreateLabelValues {
			matchValuesStr += "\"" + value + "\""
			if index < len(resInfo.autoCreateLabelValues)-1 {
				matchValuesStr += ", "
			}
		}
		selectorValue = strings.Replace(selectorValue, "{{__MATCH_VALUES__}}", matchValuesStr, -1)
	}
	template = strings.Replace(template, "{{__SELECTOR_VALUE__}}", selectorValue, -1)
	return template
}

/* Create application. Assume it does not already exist */
func createApplication(resController *ClusterWatcher, rw *ResourceWatcher, resInfo *autoCreateResourceInfo) error {
	if klog.V(4) {
		klog.Infof("Creating application %s/%s, from %s", resInfo.namespace, resInfo.autoCreateName, resInfo.name)
	}
	gvr, ok := resController.getWatchGVR(coreApplicationGVR)
	if ok {
		var intfNoNS = resController.plugin.dynamicClient.Resource(gvr)
		var intf dynamic.ResourceInterface
		if resInfo.namespace != "" {
			intf = intfNoNS.Namespace(resInfo.namespace)
		} else {
			intf = intfNoNS
		}

		template := getApplicationJSON(resInfo)

		if klog.V(4) {
			klog.Infof("createApplication JSON: %s", template)
		}
		var unstructuredObj = &unstructured.Unstructured{}
		err := unstructuredObj.UnmarshalJSON([]byte(template))
		if err != nil {
			klog.Errorf("Unable to convert JSON %s to unstructured", template)
			return err
		}

		_, err = intf.Create(unstructuredObj, metav1.CreateOptions{})
		if err != nil {
			klog.Errorf("Unable to create Application %s/%s error: %s", resInfo.namespace, resInfo.autoCreateName, err)
			return err
		}
	} else {
		err := fmt.Errorf("Unable to get GVR for Application")
		klog.Errorf("Unable to create Application %s/%s.  Error: %s", resInfo.namespace, resInfo.autoCreateName, err)
		return err
	}
	if klog.V(2) {
		klog.Infof("Created application %s/%s", resInfo.namespace, resInfo.autoCreateName)
	}
	return nil
}

func autoGenerated(appResInfo *appResourceInfo) bool {
	return appResInfo.labels[AppAutoCreated] == "true"
}

/* Set annotations and labels for an existing application
 */
func setApplicationAnnotationLabels(unstructuredObj *unstructured.Unstructured, resInfo *autoCreateResourceInfo) {
	var objMap = unstructuredObj.Object
	metadataObj, ok := objMap[METADATA]
	var metadata map[string]interface{}
	if !ok {
		metadata = make(map[string]interface{})
		objMap[METADATA] = metadata
	} else {
		metadata = metadataObj.(map[string]interface{})
	}

	var annotations map[string]interface{}
	annotationsObj, ok := metadata[ANNOTATIONS]
	if !ok {
		annotations = make(map[string]interface{})
		metadata[ANNOTATIONS] = annotations
	} else {
		annotations = annotationsObj.(map[string]interface{})
	}
	annotations[AppAutoCreatedFromName] = resInfo.name
	annotations[AppAutoCreatedFromKind] = resInfo.kind

	labels := make(map[string]interface{})
	metadata[LABELS] = labels
	labels[resInfo.autoCreateLabel] = resInfo.autoCreateName
	labels[labelName] = resInfo.autoCreateName
	labels[labelVersion] = resInfo.autoCreateVersion
	labels[labelAutoCreate] = "true"

	var spec map[string]interface{}
	specObj, ok := objMap[SPEC]
	if !ok {
		spec = make(map[string]interface{})
		objMap[SPEC] = spec
	} else {
		spec = specObj.(map[string]interface{})
	}
	var selector map[string]interface{}
	selectorObj, ok := spec[SELECTOR]
	if !ok {
		selector = make(map[string]interface{})
		spec[SELECTOR] = selector
	} else {
		selector = selectorObj.(map[string]interface{})
	}
	if len(resInfo.autoCreateLabelValues) == 1 {
		matchLabels := make(map[string]interface{})
		selector[MATCHLABELS] = matchLabels
		matchLabels[resInfo.autoCreateLabel] = resInfo.autoCreateLabelValues[0]
		delete(selector, MATCHEXPRESSIONS)
	} else {
		delete(selector, MATCHLABELS)

		matchExpressions := make([]interface{}, 0)
		expression := make(map[string]interface{})
		matchExpressions = append(matchExpressions, expression)
		selector[MATCHEXPRESSIONS] = matchExpressions

		expression[KEY] = resInfo.autoCreateLabel
		expression[OPERATOR] = OperatorIn
		values := make([]interface{}, 0)
		for _, val := range resInfo.autoCreateLabelValues {
			values = append(values, val)
		}
		expression[VALUES] = values
	}

	componentKinds := make([]interface{}, 0)
	for _, ck := range resInfo.autoCreateKinds {
		ckMap := make(map[string]interface{})
		ckMap[GROUP] = ck.group
		ckMap[KIND] = ck.kind
		componentKinds = append(componentKinds, ckMap)
	}
	spec[COMPONENTKINDS] = componentKinds
}

func sameComponentKinds(kinds1 []groupKind, kinds2 []groupKind) bool {
	if len(kinds1) != len(kinds2) {
		return false
	}
	for index, gk1 := range kinds1 {
		gk2 := kinds2[index]
		if (gk1.group != gk2.group) || (gk1.kind != gk2.kind) {
			return false
		}
	}
	return true
}

func sameStringArray(array1 []string, array2 []string) bool {
	if len(array1) != len(array2) {
		return false
	}
	for index, str1 := range array1 {
		str2 := array2[index]
		if str1 != str2 {
			return false
		}
	}
	return true
}
func stringArrayToString(array []string) string {
	ret := "["
	length := len(array)
	for index, str := range array {
		ret += "\"" + str + "\""
		if index < length-1 {
			ret += ", "
		}
	}
	ret += "]"
	return ret
}

func sameMatchExpressions(expressions1 []matchExpression, expressions2 []matchExpression) bool {

	if len(expressions1) != len(expressions2) {
		return false
	}

	for index, expr1 := range expressions1 {
		expr2 := expressions2[index]
		if expr1.key != expr2.key {
			return false
		}
		if expr1.operator != expr2.operator {
			return false
		}
		if !sameStringArray(expr1.values, expr2.values) {
			return false
		}
	}
	return true
}

func autoCreatedApplicationNeedsUpdate(appResInfo *appResourceInfo, resInfo *autoCreateResourceInfo) bool {

	/* Check if labels have changed */
	if appResInfo.labels[resInfo.autoCreateLabel] != resInfo.autoCreateName {
		if klog.V(5) {
			klog.Infof("auttoCreatedApplicationNeedsUpdate labels mismatch. Label: %s, label value in app: %s label value in original resource: %s", resInfo.autoCreateLabel, appResInfo.labels[resInfo.autoCreateLabel], resInfo.autoCreateName)
		}
		return true
	}
	if appResInfo.labels[labelVersion] != resInfo.autoCreateVersion {
		if klog.V(5) {
			klog.Infof("auttoCreatedApplicationNeedsUpdate version mismatch. version label %s in app: %s. Version in original resource: %s", labelVersion, appResInfo.labels[labelVersion], resInfo.autoCreateVersion)
		}
		return true
	}

	// check if component kinds have changed
	if !sameComponentKinds(appResInfo.componentKinds, resInfo.autoCreateKinds) {
		if klog.V(5) {
			klog.Infof("auttoCreatedApplicationNeedsUpdate component kinds mismatch. In app: %v In original resource: %v", appResInfo.componentKinds, resInfo.autoCreateKinds)
		}
		return true
	}

	/* check if selectors have changed */
	lenMatchLabels := len(appResInfo.matchLabels)
	lenMatchExpressions := len(appResInfo.matchExpressions)
	if lenMatchLabels > 0 && lenMatchExpressions > 0 {
		// shouldn't happen
		if klog.V(5) {
			klog.Infof("auttoCreatedApplicationNeedsUpdate : created app contains both match labels and expressions")
		}
		return true
	}
	if lenMatchLabels == 1 {
		if len(resInfo.autoCreateLabelValues) != 1 {
			// number of labels don't match
			if klog.V(5) {
				klog.Infof("auttoCreatedApplicationNeedsUpdate application has 1 match label value, but original resource has %d label values", len(resInfo.autoCreateLabelValues))
			}
			return true
		}
		if appResInfo.matchLabels[resInfo.autoCreateLabel] != resInfo.autoCreateLabelValues[0] {
			if klog.V(5) {
				klog.Infof("auttoCreatedApplicationNeedsUpdate match label values mismatch. Created app: %s: %s, original resource: %s", resInfo.autoCreateLabel, appResInfo.matchLabels[resInfo.autoCreateLabel], resInfo.autoCreateLabelValues[0])
			}
			// labels don't match
			return true
		}
	} else if lenMatchExpressions == 1 {
		matchExpression := appResInfo.matchExpressions[0]
		if matchExpression.key != resInfo.autoCreateLabel {
			if klog.V(5) {
				klog.Infof("autoCreatedApplicationNeedsUpdate match expression key mismatch. Created app key: %s, original resource key: %s", matchExpression.key, resInfo.autoCreateLabel)
			}
			return true
		}
		if matchExpression.operator != OperatorIn {
			if klog.V(5) {
				klog.Infof("auttoCreatedApplicationNeedsUpdate  create app operation is not %s", OperatorIn)
			}
			return true
		}

		if !sameStringArray(matchExpression.values, resInfo.autoCreateLabelValues) {
			if klog.V(5) {
				klog.Infof("auttoCreatedApplicationNeedsUpdate mismatched expression values. Create app values: %s, original resource values: %s", stringArrayToString(matchExpression.values), stringArrayToString(resInfo.autoCreateLabelValues))
			}
			return true
		}
	} else {
		// shouldn't happen
		if klog.V(5) {
			klog.Infof("auttoCreatedApplicationNeedsUpdate neither match label nor match express is length 1")
		}
		return true
	}

	return false
}

/* Return true if the resource exists */
func resourceExisting(dynamicClient dynamic.Interface, namespace string, name string, gvr schema.GroupVersionResource) bool {
	var intfNoNS = dynamicClient.Resource(gvr)
	var intf dynamic.ResourceInterface
	if namespace != "" {
		intf = intfNoNS.Namespace(namespace)
	} else {
		intf = intfNoNS
	}

	// fetch the current resource
	_, err := intf.Get(name, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("resourceExisting error: %s, type: %T", err, err)
		return false
	}
	return true
}

/* Delte auto-creqated applications  whose original resource no longer exists
 */
func deleteOrphanedAutoCreatedApplications(resController *ClusterWatcher) error {
	if klog.V(4) {
		klog.Infof("Deleting orphaned auto-crated application\n")
	}

	gvr, ok := resController.getWatchGVR(coreApplicationGVR)
	if !ok {
		err := fmt.Errorf("Unable to get GVR for Application")
		klog.Errorf("Error in deleteOrphanedAutoCreatedApplications: %s", err)
		return err
	}
	var intf = resController.plugin.dynamicClient.Resource(gvr)

	// fetch the current resource
	var unstructuredList *unstructured.UnstructuredList
	var err error
	unstructuredList, err = intf.List(metav1.ListOptions{LabelSelector: "kappnav.app.auto-created=true"})
	if err != nil {
		// TODO: check error code. Most likely resource does not exist
		return err
	}

	for _, unstructuredObj := range unstructuredList.Items {
		var appResInfo = &appResourceInfo{}
		err = resController.parseAppResource(&unstructuredObj, appResInfo)
		if err != nil {
			continue
		}
		if klog.V(4) {
			klog.Infof("    deleteOrphanedAutoCreatedApplications checking application: %s\n", appResInfo.name)
		}

		annotationsObj, ok := appResInfo.metadata[ANNOTATIONS]
		if !ok {
			continue
		}

		annotations, ok := annotationsObj.(map[string]interface{})
		if !ok {
			continue
		}

		fromName, fromKind, ok := getAutoCreatedFromParams(annotations)
		if !ok {
			continue
		}

		fromGVR, ok := resController.getWatchGVRForKind(fromKind)
		if !ok {
			if klog.V(5) {
				klog.Errorf("Error in deleteOrphanedAutoCreatedApplications: Unable to find GVR for %s", fromKind)
			}
			continue
		}
		if !resourceExisting(resController.plugin.dynamicClient, appResInfo.namespace, fromName, fromGVR) {
			if klog.V(4) {
				klog.Infof("    deleting application: %s/%s created from name: %s kind: %s\n", appResInfo.namespace, appResInfo.name, fromName, fromKind)
			}
			err := deleteResource(resController, &appResInfo.resourceInfo)
			if err != nil {
				klog.Errorf("Error deleting orphaned application:  %s/%s. Error: %s\n", appResInfo.namespace, appResInfo.name, err)
			} else {
				if klog.V(2) {
					klog.Infof("Deleted orphaned auto-created application %s/%s\n", appResInfo.namespace, appResInfo.name)
				}
			}
		}
	}

	return nil
}
