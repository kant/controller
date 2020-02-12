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
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
)

/* kAppNav Status Controller

client-go      Cluster Watcher      queue                    channel
				  +----------+    +-------------+
kind1           ->|  kind 1  |->  +rate limiting| --> handler 1 --> batchStore
store+controller  |          |    |queue        |       ^        ^    |
				  +----------+    +-------------+       |        |    |
kind2             |  kind 2  |->  |rate limiting| -------        |    V
store +controller |          |    +queue        |                |   batch
				  +----------+    +-------------+                |   processor
				  |   ...    |    | ...         | --> handler 2  +
				  + ---------+    +-------------+

  The kAppNav Status controller is designed to
  - watch over multiple kinds of resources
  - watch over kinds defined by custom resource definitions that
	 - may not be declared yet
	 - may be deleted

  The main entry point to create the kappnav status controller is
  the NewClusterWathcer method. By default, resource change events are first
  queed, and then processed by handlers.  There are three bulit-in handers:
	 - CRDHandler: to process custom resource definitions as they come and go
	 - ApplicationHandler to process kappnav application status changes
	 - default handler: to process non-application resources that can have
	   kappnav status.

  The client-go library is used to create a Kubernetes controller and cache
  for each kind. A callback handler is registered with client-go library to
  receive resource add/delete/modify events for each resource.
  The handler for each resource calulates the minimum resources that are
  affected by a resource change, and then places the information  into
  a batchStore. A separate thread fetches the affected resources from
  the batchStore to recalculate status in batches. Examples of change:
  - when a resource is changed, status needs to be recalulated on
	all the ancestor resources before change, and after change
  - when an application is changed, status needs to be recalculated
	all the ancestor resources, and for the application itself.


 After calculating minimum affected resources, the handlers place the
 resources on a channel to be sent to the batch store. A batch processor
 calls the bathStore to get resources to be processed in batches. The
 batchStore will
 - block until there are resources to be processed
 - batch up resources for up to a cofigured duration before deliverig them to
   to reduce resource usage.

 The batch processor calculates the application and resource status, and
 updates the kubernetes server if the status has changed.  If there is an
 error, resources that still exist are placed back into the bathStore
 to be processed again in the later.
*/
const (
	retryLimit = 5 // number of times to retry if the handlers encounter error

	DEPLOYMENT                 = "Deployment"
	STATEFULSET                = "StatefulSet"
	APPLICATION                = "Application"
	KAppNav                    = "KAppNav"
	KappnavUIService           = "kappnav-ui-service"
	CustomResourceDefinition   = "CustomResourceDefinition"
	OpenShiftWebConsoleConfig  = "OpenShiftWebConsoleConfig"
	OpenShiftWebConsole        = "openshift-web-console"
	V1                         = "v1"
	CONFIGMAPS                 = "configmaps"
	APIVERSION                 = "apiVersion"
	KIND                       = "kind"
	ANNOTATIONS                = "annotations"
	MATCHEXPRESSIONS           = "matchExpressions"
	KEY                        = "key"
	PLURAL                     = "plural"
	OPERATOR                   = "operator"
	SCOPE                      = "scope"
	NAMESPACED                 = "Namespaced"
	VALUES                     = "values"
	GROUP                      = "group"
	METADATA                   = "metadata"
	MATCHLABELS                = "matchLabels"
	NAME                       = "name"
	NAMES                      = "names"
	NAMESPACE                  = "namespace"
	LABELS                     = "labels"
	SPEC                       = "spec"
	VERSION                    = "version"
	SELECTOR                   = "selector"
	COMPONENTKINDS             = "componentKinds"
	statusUnknown              = "status-unknown"
	appStatusPrecedence        = "app-status-precedence"
	appNamespaces              = "app-namespaces"
	kappnavStatusValue         = "kappnav.status.value"
	kappnavStatusFlyover       = "kappnav.status.flyover"
	kappnavStatusFlyoverNls    = "kappnav.status.flyover.nls"
	defaultkAppNavNamespace    = "kappnav"
	kappnavConfig              = "kappnav-config"
	kappnavComponentNamespaces = "kappnav.component.namespaces" // annotation for additional namespaces for application components
)

// coreKindToGVR map is for backward compatibility with initial releases
// of KAppNav where specifying kind was enough. It maps a kind string
// to the default GVR for the kind.
var (
	coreKindToGVR map[string]schema.GroupVersionResource

	coreServiceGVR = schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "services",
	}
	coreDeploymentGVR = schema.GroupVersionResource{
		Group:    "apps",
		Version:  "v1",
		Resource: "deployments",
	}
	coreConfigMapGVR = schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "configmaps",
	}
	coreSecretGVR = schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "secrets",
	}
	coreVolumeGVR = schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "volumes",
	}
	coreRouteGVR = schema.GroupVersionResource{
		Group:    "route.openshift.io",
		Version:  "v1",
		Resource: "routes",
	}
	coreCustomResourceDefinitionGVR = schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1beta1",
		Resource: "customresourcedefinitions",
	}
	coreApplicationGVR = schema.GroupVersionResource{
		Group:    "app.k8s.io",
		Version:  "v1beta1",
		Resource: "applications",
	}
	coreStatefulSetGVR = schema.GroupVersionResource{
		Group:    "apps",
		Version:  "v1",
		Resource: "statefulsets",
	}
	coreIngressGVR = schema.GroupVersionResource{
		Group:    "extensions",
		Version:  "v1beta1",
		Resource: "ingresses",
	}
	coreJobGVR = schema.GroupVersionResource{
		Group:    "batch",
		Version:  "v1",
		Resource: "jobs",
	}
	coreServiceAccountGVR = schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "serviceaccounts",
	}
	coreClusterRoleGVR = schema.GroupVersionResource{
		Group:    "rbac.authorization.k8s.io",
		Version:  "v1",
		Resource: "clusterroles",
	}
	coreClusterRoleBindingGVR = schema.GroupVersionResource{
		Group:    "rbac.authorization.k8s.io",
		Version:  "v1",
		Resource: "clusterrolebindings",
	}
	coreRoleGVR = schema.GroupVersionResource{
		Group:    "rbac.authorization.k8s.io",
		Version:  "v1",
		Resource: "roles",
	}
	coreRoleBindingGVR = schema.GroupVersionResource{
		Group:    "rbac.authorization.k8s.io",
		Version:  "v1",
		Resource: "rolebindings",
	}
	coreStorageClassGVR = schema.GroupVersionResource{
		Group:    "storage.k8s.io",
		Version:  "v1",
		Resource: "storageclasses",
	}
	coreEndpointGVR = schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "endpoints",
	}
	corePersistentVolumeClaimGVR = schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "persistentvolumeclaims",
	}
	coreNodeGVR = schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "nodes",
	}
)

func init() {

	if klog.V(2) {
		klog.Infof("init")
	}

	coreKindToGVR = make(map[string]schema.GroupVersionResource)
	coreKindToGVR["Service"] = coreServiceGVR
	coreKindToGVR["Deployment"] = coreDeploymentGVR
	coreKindToGVR["Route"] = coreRouteGVR
	coreKindToGVR["ConfigMap"] = coreConfigMapGVR
	coreKindToGVR["Secret"] = coreSecretGVR
	coreKindToGVR["Volume"] = coreVolumeGVR
	coreKindToGVR["PersistentVolumeClaim"] = corePersistentVolumeClaimGVR
	coreKindToGVR["CustomResourceDefinition"] = coreCustomResourceDefinitionGVR
	coreKindToGVR["Application"] = coreApplicationGVR
	coreKindToGVR["StatefulSet"] = coreStatefulSetGVR
	coreKindToGVR["Ingress"] = coreIngressGVR
	coreKindToGVR["Job"] = coreJobGVR
	coreKindToGVR["ServiceAccount"] = coreServiceAccountGVR
	coreKindToGVR["ClusterRole"] = coreClusterRoleGVR
	coreKindToGVR["ClusterRoleBinding"] = coreClusterRoleBindingGVR
	coreKindToGVR["Role"] = coreRoleGVR
	coreKindToGVR["RoleBinding"] = coreRoleBindingGVR
	coreKindToGVR["StorageClass"] = coreStorageClassGVR
	coreKindToGVR["Endpoint"] = coreEndpointGVR
	coreKindToGVR["Node"] = coreNodeGVR
}

func logStack(msg string) {
	buf := make([]byte, 4096)
	len := runtime.Stack(buf, false)
	klog.Infof("BEGIN STACK %s\n%s\nEND STACK\n", msg, buf[:len])
}

// ControllerPlugin contains dependencies to the controller that can be mocked by unit test
type ControllerPlugin struct {
	dynamicClient   dynamic.Interface
	discoveryClient discovery.DiscoveryInterface
	batchDuration   time.Duration
	statusFunc      calculateComponentStatusFunc
}

// ClusterWatcher watches all resources for one Kube cluster
type ClusterWatcher struct {
	plugin              *ControllerPlugin
	handlerMgr          *HandlerManager
	nsFilter            *namespaceFilter
	resourceMap         map[schema.GroupVersionResource]*ResourceWatcher // all resources being watched
	gvrsToWatch         map[schema.GroupVersionResource]bool             // set of gvrs to watch for resources
	apiVersionKindToGVR sync.Map
	groupKindToGVR      sync.Map
	statusPrecedence    []string // array of status precedence
	unknownStatus       string   // value of unkown status
	namespaces          map[string]string
	resourceChannel     *resourceChannel // channel to send application updates
	mutex               sync.Mutex
}

// NewClusterWatcher creates a new ClusterWatcher
func NewClusterWatcher(controllerPlugin *ControllerPlugin) (*ClusterWatcher, error) {

	var resController = &ClusterWatcher{}
	resController.plugin = controllerPlugin
	resController.handlerMgr = newHandlerManager()
	resController.nsFilter = newNamespaceFilter()
	resController.gvrsToWatch = make(map[schema.GroupVersionResource]bool, 50)
	resController.resourceMap = make(map[schema.GroupVersionResource]*ResourceWatcher, 50)

	var err error
	resController.statusPrecedence, resController.unknownStatus, resController.namespaces, err =
		fetchDataFromConfigMap(controllerPlugin.dynamicClient)
	if err != nil {
		return nil, err
	}

	// init list of all resources
	err = resController.initResourceMap()
	if err != nil {
		return nil, err
	}

	// start batchStore to unprocessed resource changes
	resController.resourceChannel = newResourceChannel()
	batchStore := newBatchStore(resController, controllerPlugin.batchDuration)
	go batchStore.run()

	// start watch CRD
	gvr, ok := resController.getWatchGVR(coreCustomResourceDefinitionGVR)
	if !ok {
		if klog.V(4) {
			klog.Infof("NewClusterWatcher error getting GVR: %s, returning nil", coreCustomResourceDefinitionGVR)
		}
		return nil, nil
	}
	err = resController.AddToWatch(gvr)
	if err != nil {
		if klog.V(4) {
			klog.Infof("NewClusterWatcher error adding GVR: %s to watch, returning nil", coreCustomResourceDefinitionGVR)
		}
		return nil, err
	}
	if klog.V(4) {
		klog.Infof("NewClusterWatcher exit success")
	}
	return resController, nil
}

func getkAppNavNamespace() string {
	ns := os.Getenv("KAPPNAV_CONFIG_NAMESPACE")
	if ns == "" {
		ns = defaultkAppNavNamespace
	}
	return ns
}

// fetchDataFromConfigMap gets status precedence, unknown status, and application namespaces from ConfigMap Kubernetes
func fetchDataFromConfigMap(dynInterf dynamic.Interface) ([]string, string, map[string]string, error) {
	gvr := schema.GroupVersionResource{
		Group:    "",
		Version:  V1,
		Resource: CONFIGMAPS,
	}
	var intfNoNS = dynInterf.Resource(gvr)
	var intf dynamic.ResourceInterface
	intf = intfNoNS.Namespace(getkAppNavNamespace())

	// fetch the current resource
	var unstructuredObj *unstructured.Unstructured
	var err error
	unstructuredObj, err = intf.Get(kappnavConfig, metav1.GetOptions{})
	if err != nil {
		return nil, "", nil, err
	}

	var objMap = unstructuredObj.Object
	dataMap, ok := objMap["data"].(map[string]interface{})
	if !ok {
		return nil, "", nil, fmt.Errorf("Configmap kappnav-config does not not contain \"data\" property")
	}
	unknownStatObj, ok := dataMap[statusUnknown]
	if !ok {
		return nil, "", nil, fmt.Errorf("Configmap kappnav-config does not contain status-unknown property")
	}
	unknownStat, ok := unknownStatObj.(string)
	if !ok {
		return nil, "", nil, fmt.Errorf("Configmap kappnav-config status-unknown not a string")
	}

	appStatPreced, ok := dataMap[appStatusPrecedence]
	if !ok {
		return nil, "", nil, fmt.Errorf("Configmap kappnav-config does not contain app-status-precedence property")
	}

	statusPrecedence, ok := appStatPreced.(string)
	if !ok {
		return nil, "", nil, fmt.Errorf("Configmap kappnav-config app-status-precedence not a JSON array")
	}
	ret, err := jsonToArrayOfString(statusPrecedence)
	if err != nil {
		return nil, "", nil, fmt.Errorf("In ConfigMap kappnav-config, the value of app-status-precedence not valid JSON: %s, parsing error: %s", statusPrecedence, err)
	}

	namespaces := make(map[string]string)
	appNamespaces, ok := dataMap[appNamespaces]
	if ok {
		appNamespacesStr, ok := appNamespaces.(string)
		if !ok {
			return nil, "", nil, fmt.Errorf("Configmap kappnav-config app-namespaces is not a string")
		}
		namespaces = stringToNamespaceMap(appNamespacesStr)
	}
	return ret, unknownStat, namespaces, nil
}

func jsonToArrayOfString(str string) ([]string, error) {
	bytes := []byte(str)
	var interf interface{}
	err := json.Unmarshal(bytes, &interf)
	if err != nil {
		return nil, err
	}

	var objArray []interface{}
	objArray, ok := interf.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value is not an array")
	}

	ret := make([]string, 0, len(objArray))
	for _, val := range objArray {
		tmp, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("value is not an array of string")
		}
		ret = append(ret, tmp)
	}
	return ret, nil
}

// Get cached status precedence
func (resController *ClusterWatcher) getStatusPrecedence() []string {
	return resController.statusPrecedence
}

// Return true if a gvr is namespaced
func (resController *ClusterWatcher) isNamespaced(gvr schema.GroupVersionResource) bool {
	if klog.V(4) {
		klog.Infof("isNamespaced %s", gvr)
	}
	resController.mutex.Lock()
	defer resController.mutex.Unlock()

	ret := false
	if rw := resController.resourceMap[gvr]; rw != nil {
		ret = rw.namespaced
	} else {
		// TODO: in theory checking whether a reousrce is namespaced
		// when resource doesn't exist shouldn't happen
	}

	if klog.V(4) {
		klog.Infof("isNamespaced %s: %t", gvr, ret)
	}
	return ret
}

// isNamespacePermitted returns true if resources in given namespace are allowed in this kappnav instance
func (resController *ClusterWatcher) isNamespacePermitted(namespace string) bool {
	if klog.V(4) {
		klog.Infof("isNamespacePermitted %s", namespace)
	}
	var ret = false

	if len(resController.namespaces) == 0 {
		// No namespaces means all resources
		ret = true
	} else if _, ok := resController.namespaces[namespace]; ok {
		// Namespace must be in kappnav-config Configmap app-namespaces
		ret = true
	}
	if klog.V(4) {
		klog.Infof("isNamespacePermitted %s: %t", namespace, ret)
	}
	return ret
}

// isEventPermitted returns true if the event object namespace is allowed in this kappnav instance
func (resController *ClusterWatcher) isEventPermitted(eventData *eventHandlerData) bool {
	var ret = false
	namespace, ok := getNamespace(eventData.obj)
	if ok {
		ret = resController.isNamespacePermitted(namespace)
	} else {
		ret = true
	}
	if klog.V(4) {
		klog.Infof("isEventPermitted gvr: %s key: %s: return: %t", eventData.gvr, eventData.key, ret)
	}
	return ret
}

// isAllNamespacesPermitted returns true if resources in all namespaces are allowed in this kappnav instance
func (resController *ClusterWatcher) isAllNamespacesPermitted() bool {
	if klog.V(4) {
		klog.Infof("isAllNamespacesPermitted")
	}
	var ret = len(resController.namespaces) == 0
	if klog.V(4) {
		klog.Infof("isAllNamespacesPermitted %t:", ret)
	}
	return ret
}

// AddToWatch adds a GVR to the watch list
func (resController *ClusterWatcher) AddToWatch(gvr schema.GroupVersionResource) error {
	if klog.V(3) {
		klog.Infof("AddToWatch %s\n", gvr)
	}

	resController.mutex.Lock()
	resController.gvrsToWatch[gvr] = true
	resController.mutex.Unlock()

	// start watching this GVR
	return resController.startWatch(gvr)
}

// ResourceWatcher stores information about one GVR being watched.
// In client-go, each GVR has its own cache.
type ResourceWatcher struct {
	schema.GroupVersionResource
	kind         string
	namespaced   bool              // true if resource has namespace
	subResources map[string]string // all the sub resources, e.g., "status"

	store      cache.Store
	controller cache.Controller
	indexer    cache.Indexer
	queue      workqueue.RateLimitingInterface // queued up events on resources
	stopCh     chan struct{}                   // channel to stop the controller for this resource
	// handler *cache.ResourceEventHandlerFuncs
	// handler *resourceActionFunc // callback
}

// Callback function to be implemeted
type resourceActionFunc func(resController *ClusterWatcher, rw *ResourceWatcher, eventData *eventHandlerData) error

// process next item on the queue. Return false if queue is closed
func processNextItem(resController *ClusterWatcher, watcher *ResourceWatcher /*, handler *resourceActionFunc*/) bool {

	// Wait until there is a new item in the queue
	tmp, quit := watcher.queue.Get()
	if quit {
		if klog.V(2) {
			klog.Infof("queue for GVR %s closed", watcher.GroupVersionResource)
		}
		return false
	}
	defer watcher.queue.Done(tmp)

	handlerData := tmp.(*eventHandlerData)
	if klog.V(4) {
		klog.Infof("processing %s, GVR %s from queue", handlerData.key, watcher.GroupVersionResource)
	}

	// call handler to process the data
	// err := (*handler)(resController, watcher, handlerData)
	err := resController.handlerMgr.callHandlers(watcher.GroupVersionResource, resController, watcher, handlerData)
	handleError(watcher, err, handlerData)
	return true
}

// Handle potential error when proessing resource from the queue
func handleError(watcher *ResourceWatcher, err error, handlerData *eventHandlerData) {
	if err == nil {
		// no error
		watcher.queue.Forget(handlerData)
		return
	}

	if watcher.queue.NumRequeues(handlerData) < retryLimit {
		// requeue for retry
		if klog.V(4) {
			klog.Errorf("Error processing %v: %v. Requeueing", handlerData.key, err)
		}
		watcher.queue.AddRateLimited(handlerData)
		return
	}

	// reached limit on retry
	watcher.queue.Forget(handlerData)
	utilruntime.HandleError(fmt.Errorf("Retry limit reached. Unable to process %q due to error %v", handlerData.key, err))
}

// getWatchGVRForKind gets the currently watched GVR for a core kind
func (resController *ClusterWatcher) getWatchGVRForKind(kind string) (schema.GroupVersionResource, bool) {
	gvr, ok := coreKindToGVR[kind]
	if !ok {
		klog.Infof("getWatchGVRForKind kind: %s is not a core kappnav kind", kind)
		return schema.GroupVersionResource{}, false
	}
	return resController.getWatchGVR(gvr)
}

// getWatchGVR gets the GVR from the resourceWatcher. It returns false if there is no resourceWatcher
// or the input GVR is different from the watched GVR
func (resController *ClusterWatcher) getWatchGVR(gvr schema.GroupVersionResource) (schema.GroupVersionResource, bool) {
	resController.mutex.Lock()
	rw, ok := resController.resourceMap[gvr]
	resController.mutex.Unlock()
	if ok {
		if rw.GroupVersionResource != gvr {
			if klog.V(4) {
				klog.Infof("getWatchGVR watched GVR: %s different from input GVR: %s", rw.GroupVersionResource, gvr)
			}
		}
		return rw.GroupVersionResource, true
	}
	if klog.V(4) {
		klog.Infof("getWatchGVR GVR: %s not found in resourceMap returning false", gvr)
	}
	return schema.GroupVersionResource{}, false
}

// getGVRForGroupKind gets the GVR for a kind and group
func (resController *ClusterWatcher) getGVRForGroupKind(group string, kind string) (schema.GroupVersionResource, bool) {

	// map group/kind to apiVersion

	groupKind := group + "/" + kind
	if klog.V(2) {
		klog.Infof("getGVRForGroupKind trying group/Kind: %s", groupKind)
	}
	gvr, ok := resController.groupKindToGVR.Load(groupKind)
	if ok {
		if klog.V(2) {
			klog.Infof("getGVRForGroupKind for group: %s kind: %s returning: %v", group, kind, gvr.(schema.GroupVersionResource))
		}
		return gvr.(schema.GroupVersionResource), true
	}
	if klog.V(2) {
		klog.Infof("getGVRForGroupKind WARNING: No CRD found with group: " + group + " for kind: " + kind)
	}
	// no CRDs installed with the specified group/kind
	// See if it's one of the core kinds for compatibility
	gvr, ok = coreKindToGVR[kind]
	if ok {
		if klog.V(2) {
			klog.Infof("getGVRForGroupKind returning default GVR for group: %s kind: %s GVR: %v", group, kind, gvr.(schema.GroupVersionResource))
		}
		return gvr.(schema.GroupVersionResource), true
	}
	if klog.V(2) {
		klog.Infof("getGVRForGroupKind no CRD found for group: %s kind: %s, returning false", group, kind)
	}
	return schema.GroupVersionResource{}, false
}

// get resource watcher
func (resController *ClusterWatcher) getResourceWatcher(gvr schema.GroupVersionResource) *ResourceWatcher {
	resController.mutex.Lock()
	defer resController.mutex.Unlock()
	return resController.resourceMap[gvr]
}

// list resources for a gvr. Return empty array if resource is not being watched.
func (resController *ClusterWatcher) listResources(gvr schema.GroupVersionResource) []interface{} {
	resController.mutex.Lock()
	rw, ok := resController.resourceMap[gvr]
	resController.mutex.Unlock()

	if ok && rw.store != nil {
		return rw.store.List()
	}
	return make([]interface{}, 0)
}

// Get a resource from the cache
// Return:
//     pionter to resource
//     true if resource exists
//     error ecountered to get the resource
func (resController *ClusterWatcher) getResource(gvr schema.GroupVersionResource, namespace string, name string) (interface{}, bool, error) {
	resController.mutex.Lock()
	rw, ok := resController.resourceMap[gvr]
	var store cache.Store
	if ok {
		store = rw.store
	}
	resController.mutex.Unlock()

	if ok && store != nil {
		key := name
		if namespace != "" {
			key = namespace + "/" + key
		}
		return store.GetByKey(key)
	}
	return nil, false, fmt.Errorf("GetResource unable to find resources %s %s %s", gvr, namespace, name)
}

// add a new entry to resource map
func (resController *ClusterWatcher) addResourceMapEntry(kind string, group string, version string, plural string, namespaced bool) {

	// if kind == "" || strings.HasPrefix(plural, "%") {
	// 	klog.Infof("addResourceMapEntry skipping resource kind: %s, group: %s, version: %s, plural: %s namespaced: %t", logString(kind), logString(group), logString(version), logString(plural), namespaced)
	// 	return
	// }
	if klog.V(3) {
		klog.Infof("addResourceMapEntry entry resource kind: %s, group: %s, version: %s, plural: %s namespaced: %t", logString(kind), logString(group), logString(version), logString(plural), namespaced)
	}
	var subResource string
	if strings.Contains(plural, "/") {
		split := strings.Split(plural, "/")
		plural = split[0]
		subResource = split[1]
	}
	resController.mutex.Lock()
	gvr := schema.GroupVersionResource{Group: group, Version: version, Resource: plural}
	rw, ok := resController.resourceMap[gvr]
	if !ok {
		// create new entry
		rw = &ResourceWatcher{}
		resController.resourceMap[gvr] = rw
	}
	apiVersionKind := version + "/" + kind
	if group != "" {
		apiVersionKind = group + "/" + apiVersionKind
		groupKind := group + "/" + kind
		// don't replace an existing entry for a core GVR
		store := true
		gvr1, ok := resController.groupKindToGVR.Load(groupKind)
		if ok {
			coreGVR, ok := coreKindToGVR[kind]
			if ok && coreGVR == gvr1 {
				if klog.V(2) {
					klog.Infof("addResourceMapEntry not repacing group/Kind map core GVR: %s with GVR: %v", coreGVR, gvr1)
				}
				store = false
			}
		}
		if store == true {
			if klog.V(2) {
				klog.Infof("addResourceMapEntry mapping group/Kind: %s to GVR: %v", groupKind, gvr)
			}
			resController.groupKindToGVR.Store(groupKind, gvr)
		}
	}
	if klog.V(2) {
		klog.Infof("addResourceMapEntry mapping apiVersion/Kind: %s to GVR: %v", apiVersionKind, gvr)
	}
	resController.apiVersionKindToGVR.Store(apiVersionKind, gvr)
	rw.Group = group
	rw.Version = version
	rw.Resource = plural
	rw.kind = kind
	rw.namespaced = namespaced
	rw.subResources = map[string]string{}
	if subResource != "" {
		// just store subresource
		rw.subResources[subResource] = subResource
		resController.mutex.Unlock()
	} else {
		// Watch the resource if it should be watched
		resController.mutex.Unlock()
		resController.restartWatch(rw.GroupVersionResource)
	}
	if klog.V(3) {
		klog.Infof("addResourceMapEntry exit")
	}
}

// Delete a resource map entry, when the resource definition is deleted
func (resController *ClusterWatcher) deleteResourceMapEntry(gvr schema.GroupVersionResource, kind string) {
	resController.stopWatch(gvr)

	resController.mutex.Lock()
	defer resController.mutex.Unlock()
	_, ok := resController.resourceMap[gvr]
	if ok {
		// can be deleted
		delete(resController.resourceMap, gvr)
	}
	apiVersionKind := gvr.Version + "/" + kind
	if gvr.Group != "" {
		apiVersionKind = gvr.Group + "/" + apiVersionKind
		groupKind := gvr.Group + "/" + kind
		// don't delete an existing group/kind map entry for a different GVR (i.e. with a different version)
		delete := true
		existingGvr, ok := resController.groupKindToGVR.Load(groupKind)
		if ok {
			if ok && existingGvr != gvr {
				if klog.V(2) {
					klog.Infof("deleteResourceMapEntry group/Kind map GVR: %s is not the GVR to be deleted: %s", existingGvr, gvr)
				}
				delete = false
			}
		}
		if delete == true {
			if klog.V(2) {
				klog.Infof("deleteResourceMapEntry deleting group/Kind: %s for GVR: %s", groupKind, gvr)
			}
			resController.groupKindToGVR.Delete(apiVersionKind)
		}
	}
	if klog.V(2) {
		klog.Infof("deleteResourceMapEntry deleting apiVersion/Kind: %s for GVR: %s", apiVersionKind, gvr)
	}
	resController.apiVersionKindToGVR.Delete(apiVersionKind)

}

// print a resourceMapEntry
func (resController *ClusterWatcher) printResourceMapEntry(gvr schema.GroupVersionResource) {
	resController.mutex.Lock()
	rw, ok := resController.resourceMap[gvr]
	var store cache.Store
	if ok {
		store = rw.store
	}
	resController.mutex.Unlock()
	if ok {
		keys := store.ListKeys()
		if klog.V(2) {
			klog.Infof("printResourceMapEntry for gvr %s\n", gvr)
			klog.Infof("    keys: %s\n", keys)
		}
	} else {
		klog.Infof("printResourceMapEntry gvr %s not found in resourceMap\n", gvr)
	}
}

func printAPIGroupList(list *metav1.APIGroupList) {
	if klog.V(2) {
		klog.Infof("printAPIGroupList\n")
	}
	klog.Infof("    kind: %s, APIVersion %s\n", list.Kind, list.APIVersion)
	for index, group := range list.Groups {
		if klog.V(2) {
			klog.Infof("    %d kind: %s APIVersion %s Name %s\n", index, group.Kind, group.APIVersion, group.Name)
		}
		for _, version := range group.Versions {
			if klog.V(2) {
				klog.Infof("        groupVersion: %s, version: %s\n", version.GroupVersion, version.Version)
			}
		}
	}
}

/* Initialize list of resource group/api/version */
func (resController *ClusterWatcher) initResourceMap() error {
	if klog.V(2) {
		klog.Infof("initResourceMap entry")
	}
	var discClient = resController.plugin.discoveryClient
	var apiGroups *metav1.APIGroupList
	apiGroups, err := discClient.ServerGroups()
	if err != nil {
		return err
	}
	if klog.V(4) {
		printAPIGroupList(apiGroups)
	}
	var groups = apiGroups.Groups
	var group metav1.APIGroup
	for _, group = range groups {

		for i := 0; i < len(group.Versions); i++ {
			if klog.V(2) {
				klog.Infof("initResourceMap processing GroupVersion %s", group.Versions[i].GroupVersion)
			}
			resController.processResourceGroup(group.Versions[i].GroupVersion, discClient)
		}

	}
	if klog.V(2) {
		klog.Infof("initResourceMap exit")
	}
	return nil
}

func (resController *ClusterWatcher) processResourceGroup(groupVersion string, discClient discovery.DiscoveryInterface) error {
	apiResourceList, err := discClient.ServerResourcesForGroupVersion(groupVersion)
	if err != nil {
		// If this groupVersion is not available, then disregard it
		klog.Errorf("initResourceMap unable to get information about APIGroup %s, skipping", groupVersion)
		return err
	}
	var group string
	var version string
	if strings.Contains(groupVersion, "/") {
		gv := strings.Split(groupVersion, "/")
		group = gv[0]
		version = gv[1]
	} else {
		group = ""
		version = groupVersion
	}
	for _, apiResource := range apiResourceList.APIResources {
		var plural = apiResource.Name
		resController.addResourceMapEntry(apiResource.Kind, group, version, plural, apiResource.Namespaced)
	}
	return nil
}

/* Resource events to be queued */
type eventHandlerFuncType int

const (
	// AddFunc - function type for resource add events
	AddFunc eventHandlerFuncType = iota
	// UpdateFunc - function type for resource update events
	UpdateFunc
	// DeleteFunc - function type for resource delete events
	DeleteFunc
)

type eventHandlerData struct {
	funcType eventHandlerFuncType
	kind     string
	gvr      schema.GroupVersionResource
	key      string
	obj      interface{}
	oldObj   interface{} // for UpdateFunc
}

// Start watch on a GVR, if it should be watched, and not already being watched
// Otherwise, noop
func (resController *ClusterWatcher) startWatch(inputGVR schema.GroupVersionResource) error {

	if klog.V(4) {
		klog.Infof("startWatch entry group: %s version: %s resource: %s\n    groupversion: %s groupresource: %s", inputGVR.Group, inputGVR.Version, inputGVR.Resource, inputGVR.GroupVersion(), inputGVR.GroupResource())
	}

	resController.mutex.Lock()
	defer resController.mutex.Unlock()

	var gvr = inputGVR // will be used by the ResourceEventHanderFuncs below

	// not ready to watch this GVR
	if !resController.gvrsToWatch[gvr] {
		if klog.V(4) {
			klog.Infof("startWatch returning nil, no gvrsToWatch")
		}
		return nil
	}

	rw, ok := resController.resourceMap[gvr]
	if !ok {
		// no entry for this resource GVR yet
		if klog.V(4) {
			klog.Infof("startWatch returning nil, no resourceMap entry for gvr: %v", gvr)
		}
		return nil
	}
	if rw.controller != nil {
		// already being watched
		if klog.V(4) {
			klog.Infof("startWatch returning nil, rw.controller already exists")
		}
		return nil
	}
	if klog.V(2) {
		klog.Infof("new startWatch GVR: %v  kind: %s\n", rw.GroupVersionResource, rw.kind)
	}

	// Set up call back functions to queue resource change events
	rw.queue = workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	rw.store, rw.controller = cache.NewIndexerInformer(
		createListWatcher(resController.plugin.dynamicClient, gvr),
		nil,
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				key, err := cache.MetaNamespaceKeyFunc(obj)
				if err == nil {
					eventObj := &eventHandlerData{
						funcType: AddFunc,
						kind:     rw.kind,
						gvr:      gvr,
						key:      key,
						obj:      obj,
					}
					rw.queue.Add(eventObj)
				}
			},
			UpdateFunc: func(old, obj interface{}) {
				key, err := cache.MetaNamespaceKeyFunc(obj)
				if err == nil {
					eventObj := &eventHandlerData{
						funcType: UpdateFunc,
						kind:     rw.kind,
						gvr:      gvr,
						key:      key,
						obj:      obj,
						oldObj:   old,
					}
					rw.queue.Add(eventObj)
				}
			},
			DeleteFunc: func(obj interface{}) {
				key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
				if err == nil {
					eventObj := &eventHandlerData{
						funcType: DeleteFunc,
						kind:     rw.kind,
						gvr:      gvr,
						key:      key,
						obj:      obj,
					}
					rw.queue.Add(eventObj)
				}
			},
		}, cache.Indexers{})

	rw.stopCh = make(chan struct{})

	if klog.V(2) {
		klog.Infof("startWatch starting cache.controller.Run() thread%v", rw.controller)
	}
	go rw.controller.Run(rw.stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(rw.stopCh, rw.controller.HasSynced) {
		err := fmt.Errorf("startWatch timed out waiting for caches to sync for GVR %s", gvr)
		klog.Error(err)
		return err
	}

	// run until queue is closed
	var theResController = resController
	var theRW = rw
	go func() {
		if klog.V(3) {
			klog.Infof("wstartWatch orker thread started for GVR: %s  input GVR: %v", theRW.GroupVersionResource, gvr)
		}
		for processNextItem(theResController, theRW) {
		}
		if klog.V(3) {
			klog.Infof("startWatch worker thread stopped for GVR: %s  input GVR: %v", theRW.GroupVersionResource, gvr)
		}
	}()

	if klog.V(4) {
		klog.Infof("    startWatch completed for GVR: %s\n", gvr)
	}
	return nil
}

// Stop watch if it's already being watched
// Start watch if it should be watched
func (resController *ClusterWatcher) restartWatch(gvr schema.GroupVersionResource) error {
	if klog.V(4) {
		klog.Infof("restartWatch %v\n", gvr)
	}
	resController.stopWatch(gvr)
	return resController.startWatch(gvr)
}

// stop watch if it's already being watched
// otherwise, noop
func (resController *ClusterWatcher) stopWatch(gvr schema.GroupVersionResource) {

	if klog.V(4) {
		klog.Infof("stopWatch %s\n", gvr)
	}
	resController.mutex.Lock()
	defer resController.mutex.Unlock()
	rw, ok := resController.resourceMap[gvr]
	if ok {
		if rw.controller != nil {
			rw.stopCh <- struct{}{} // send an empty struct
			close(rw.stopCh)
			rw.queue.ShutDown()
			rw.controller = nil
			if klog.V(2) {
				klog.Infof("stopped watching %s", gvr)
			}
		}
	}
}

// Shutdown this instance of the controller
func (resController *ClusterWatcher) shutDown() {
	// close downstream channel
	resController.resourceChannel.close()

	resController.mutex.Lock()
	// make a copy of the gvrs for sychronziation purpose*/
	gvrs := make([]schema.GroupVersionResource, 0, len(resController.resourceMap))
	for gvr := range resController.resourceMap {
		gvrs = append(gvrs, gvr)
	}
	resController.mutex.Unlock()

	// stop watch all the gvrs
	for _, gvr := range gvrs {
		resController.stopWatch(gvr)
	}
}

// Parsed information about a resource
type resourceInfo struct {
	unstructuredObj *unstructured.Unstructured // the unstructured obj for this resource
	metadata        map[string]interface{}
	apiVersion      string
	kind            string
	gvr             schema.GroupVersionResource
	labels          map[string]string
	annotations     map[string]interface{}
	namespace       string
	name            string
	kappnavStatVal  string // value of kappnav status
	flyOver         string // value of flyover text
	flyOverNLS      string // NLS string for flyover
}

// unique key for the resource.
func (resInfo *resourceInfo) key() string {
	return resInfo.gvr.String() + "/" + resInfo.namespace + "/" + resInfo.name
}

type groupKind struct {
	group string
	kind  string
	gvr   schema.GroupVersionResource
}

const (
	// OperatorIn - label matches expression
	OperatorIn = "In"
	// OperatorNotIn - label does not match expression
	OperatorNotIn = "NotIn"
	// OperatorExists - label exists
	OperatorExists = "Exists"
	// OperatorDoesNotExist - label does not exist
	OperatorDoesNotExist = "DoesNotExist"
)

type matchExpression struct {
	key      string
	operator string // In, NotIn, Exists, and DoesNotExist
	values   []string
}

// Application resource fields
type appResourceInfo struct {
	resourceInfo
	componentNamespaces map[string]string // additional namespaces for namespaced component gvrs
	componentKinds      []groupKind
	matchLabels         map[string]string // the match labels for this application
	matchExpressions    []matchExpression
}

func isSameResource(res1 *resourceInfo, res2 *resourceInfo) bool {
	return strings.Compare(res1.apiVersion, res2.apiVersion) == 0 &&
		strings.Compare(res1.gvr.String(), res2.gvr.String()) == 0 &&
		strings.Compare(res1.name, res2.name) == 0
}

// Set the kappnav status into the resource object
func setkAppNavStatus(unstructuredObj *unstructured.Unstructured, stat string, flyoverText string, flyoverNLS string) {
	var objMap = unstructuredObj.Object
	var metadata = objMap[METADATA].(map[string]interface{})

	if klog.V(4) {
		klog.Infof("setkAppNavStatus resource: %s status: %s flyover:%s\n", unstructuredObj.GetName(), stat, flyoverText)
	}

	annotationsInterf, ok := metadata[ANNOTATIONS]
	var annotations map[string]interface{}
	if !ok {
		// annotations does not exist
		annotations = make(map[string]interface{})
		metadata[ANNOTATIONS] = annotations
	} else {
		annotations = annotationsInterf.(map[string]interface{})
	}
	annotations[kappnavStatusValue] = stat
	annotations[kappnavStatusFlyover] = flyoverText
	annotations[kappnavStatusFlyoverNls] = flyoverNLS
}

// parseResource parses a resource into a structure
func (resController *ClusterWatcher) parseResource(unstructuredObj *unstructured.Unstructured, resourceInfo *resourceInfo) {
	parseResourceBasic(unstructuredObj, resourceInfo)
	apiVersionKind := resourceInfo.apiVersion + "/" + resourceInfo.kind
	gvr, ok := resController.apiVersionKindToGVR.Load(apiVersionKind)
	if ok {
		if klog.V(4) {
			klog.Infof("parseResource got gvr: %s mapped to apiVersion/Kind: %s", gvr.(schema.GroupVersionResource), apiVersionKind)
		}
		resourceInfo.gvr = gvr.(schema.GroupVersionResource)
	} else {
		if klog.V(4) {
			klog.Infof("parseResource no GVR is mapped to apiVersion/Kind: %s", apiVersionKind)
		}
	}
}

func parseResourceBasic(unstructuredObj *unstructured.Unstructured, resourceInfo *resourceInfo) {

	resourceInfo.unstructuredObj = unstructuredObj
	var objMap = unstructuredObj.Object
	resourceInfo.apiVersion = objMap[APIVERSION].(string)
	if klog.V(4) {
		klog.Infof("parseResourceBasic apiVersion: %s", resourceInfo.apiVersion)
	}
	resourceInfo.kind = objMap[KIND].(string)

	metadataObj, ok := objMap[METADATA]
	if !ok {
		resourceInfo.metadata = make(map[string]interface{})
	} else {
		resourceInfo.metadata, ok = metadataObj.(map[string]interface{})
		if !ok {
			resourceInfo.metadata = make(map[string]interface{})
		}
	}

	var annotations map[string]interface{}
	annotations, ok = resourceInfo.metadata[ANNOTATIONS].(map[string]interface{})
	if ok {
		resourceInfo.annotations = annotations
		var kappnavStat interface{}
		kappnavStat, ok = annotations[kappnavStatusValue]
		if ok && (kappnavStat != nil) {
			resourceInfo.kappnavStatVal = kappnavStat.(string)
		}
		var flyOver interface{}
		flyOver, ok = annotations[kappnavStatusFlyover]
		if ok && (flyOver != nil) {
			resourceInfo.flyOver = flyOver.(string)
		}
		var flyOverNLS interface{}
		flyOverNLS, ok = annotations[kappnavStatusFlyoverNls]
		if ok && (flyOverNLS != nil) {
			resourceInfo.flyOverNLS = flyOverNLS.(string)
		}
	} else {
		resourceInfo.annotations = make(map[string]interface{})
	}
	var labels map[string]interface{}
	resourceInfo.labels = make(map[string]string)
	labels, ok = resourceInfo.metadata[LABELS].(map[string]interface{})
	if ok {
		for key, val := range labels {
			resourceInfo.labels[key] = val.(string)
		}
	}
	resourceInfo.name = resourceInfo.metadata[NAME].(string)
	var ns interface{}
	ns, ok = resourceInfo.metadata[NAMESPACE]
	if ok {
		resourceInfo.namespace = ns.(string)
	} else {
		resourceInfo.namespace = ""
	}
}

// parseAppResource parses Application resource into more convenient representation
func (resController *ClusterWatcher) parseAppResource(unstructuredObj *unstructured.Unstructured, appResource *appResourceInfo) error {
	if klog.V(4) {
		klog.Infof("parseAppResource entry resource :%s %v", unstructuredObj.GetName(), unstructuredObj)
	}
	resController.parseResource(unstructuredObj, &appResource.resourceInfo)

	componentNS := ""
	tmp, ok := appResource.resourceInfo.annotations[kappnavComponentNamespaces]
	if ok {
		componentNS, _ = tmp.(string)
	}
	// Get namespaces this Application is limited to
	var componentNamespaces = stringToNamespaceMap(componentNS)
	appResource.componentNamespaces = make(map[string]string)
	for _, ns := range componentNamespaces {
		if resController.isNamespacePermitted(ns) {
			appResource.componentNamespaces[ns] = ns
		}
	}
	// If namespace list exists add this Application's namespace to the list
	if len(appResource.componentNamespaces) > 0 &&
		resController.isNamespacePermitted(appResource.resourceInfo.namespace) {
		appResource.componentNamespaces[appResource.resourceInfo.namespace] = appResource.resourceInfo.namespace
	}

	var objMap = unstructuredObj.Object
	var spec map[string]interface{}
	tmp, ok = objMap[SPEC]
	if !ok {
		return fmt.Errorf("object has no spec %s", unstructuredObj)
	}
	spec = tmp.(map[string]interface{})
	appResource.componentKinds = make([]groupKind, 0)
	tmp, ok = spec[COMPONENTKINDS]
	if ok {
		componentKinds := tmp.([]interface{})
		for _, component := range componentKinds {
			var kindMap = component.(map[string]interface{})
			if klog.V(4) {
				klog.Infof("parseAppResource application: %s kindMap: %v", appResource.name, kindMap)
			}
			group, _ := kindMap[GROUP].(string)
			kind, ok2 := kindMap[KIND].(string)
			if ok2 {
				if klog.V(4) {
					klog.Infof("parseAppResource application: %s processing componentKind: group: %s  kind: %s", appResource.name, group, kind)
				}
				gvr, ok3 := resController.getGVRForGroupKind(group, kind)
				if ok3 {
					if group == "" {
						group = "/" + gvr.Version
						if klog.V(4) {
							klog.Infof("parseAppResource application: %s setting group to /gvr.Version: %s", appResource.name, group)
						}
					}
					var groupKind = groupKind{
						group: group,
						kind:  kind,
						gvr:   gvr,
					}
					if klog.V(4) {
						klog.Infof("parseAppResource application: %s groupKind: %v", appResource.name, groupKind)
					}
					appResource.componentKinds = append(appResource.componentKinds, groupKind)
				} else {
					if klog.V(4) {
						klog.Infof("parseAppResource application: %s error getting GVR for componentKind: group: %s kind: %s", appResource.name, group, kind)
					}
				}
			}
		}
	}
	appResource.matchLabels = make(map[string]string)
	appResource.matchExpressions = make([]matchExpression, 0)
	var selector map[string]interface{}
	tmp, ok = spec[SELECTOR]
	if !ok {
		// no selector
		return nil
	}
	selector = tmp.(map[string]interface{})
	tmp, ok = selector[MATCHLABELS]
	if ok {
		matchLabels := tmp.(map[string]interface{})
		for key, val := range matchLabels {
			appResource.matchLabels[key] = val.(string)
		}
	}

	tmp, ok = selector[MATCHEXPRESSIONS]
	if ok {
		matchExpressions := tmp.([]interface{})
		for _, tmpExpr := range matchExpressions {
			expr := tmpExpr.(map[string]interface{})
			tmp, ok := expr[KEY]
			if !ok {
				return nil
			}
			key := tmp.(string)
			tmp, ok = expr[OPERATOR]
			if !ok {
				return nil
			}
			operator := tmp.(string)
			var values = make([]string, 0)
			tmp, ok = expr[VALUES]
			if ok {
				tmpArr := tmp.([]interface{})
				for _, elem := range tmpArr {
					values = append(values, elem.(string))
				}
			}
			var theExpr = matchExpression{
				key:      key,
				operator: operator,
				values:   values,
			}
			appResource.matchExpressions = append(appResource.matchExpressions, theExpr)
		}
	}
	return nil
}

// Get group, version, plural, kind, and subresouces defined by CRD
func getCRDGVRKindSubresource(unstructuredObj *unstructured.Unstructured) (group string, version string, plural string, kind string, namespaced bool, subResource string) {
	var objMap = unstructuredObj.Object
	var spec = objMap[SPEC].(map[string]interface{})
	group = spec[GROUP].(string)
	var names = spec[NAMES].(map[string]interface{})
	kind = names[KIND].(string)
	plural = names[PLURAL].(string)
	version = spec[VERSION].(string)
	scope, ok := spec[SCOPE]
	namespaced = false
	if ok {
		scopeStr, ok := scope.(string)
		if ok && scopeStr == NAMESPACED {
			namespaced = true
		}
	}
	subResource = ""
	if klog.V(4) {
		klog.Infof("getCRDGVRKindSubresource returning group: %s version: %s plural: %s kind: %s namespaced %t", group, version, plural, kind, namespaced)
	}
	return
}

// Add/modify a GVR of resource to be watched
func (resController *ClusterWatcher) addGVR(obj interface{}) (gvr schema.GroupVersionResource) {
	if klog.V(4) {
		klog.Infof("addGVR entry")
	}
	switch obj.(type) {
	case *unstructured.Unstructured:
		var unstructuredObj = obj.(*unstructured.Unstructured)
		group, version, plural, kind, namespaced, _ := getCRDGVRKindSubresource(unstructuredObj)
		if group != "" {
			// not part of base Kubernetes
			resController.addResourceMapEntry(kind, group, version, plural, namespaced)
		}
		gvr := schema.GroupVersionResource{Group: group, Version: version, Resource: plural}
		if klog.V(4) {
			klog.Infof("addGVR returning GVR: %s", gvr)
		}
		return gvr
	default:
		klog.Errorf("addGVR not Unstructured: type: %T val: %s\n", obj, obj)
		return schema.GroupVersionResource{}
	}
}

// Modify kind to resource map
/*
func (resController *ClusterWatcher) modifyKind(obj interface{} ) (kind string) {
	switch  obj.(type) {
		case *unstructured.Unstructured:
			var unstructuredObj *unstructured.Unstructured = obj.(*unstructured.Unstructured)
			group, version, plural, kind, _ := getCRDGVRKindSubresource(unstructuredObj)
			if group != "" {
				resController.addResourceMapEntry(kind, group, version, plural)
			}
			return kind
		 default:
			klog.Errorf("CRDHandler.addNewKind: not Unstructured: type: %T val: %s\n",  obj, obj);
			return ""
	}
}
*/

// Delete a GVR from resource map
func (resController *ClusterWatcher) deleteGVR(obj interface{}) (gvr schema.GroupVersionResource) {
	switch obj.(type) {
	case *unstructured.Unstructured:
		var unstructuredObj = obj.(*unstructured.Unstructured)
		var gvr schema.GroupVersionResource
		group, version, plural, kind, _, _ := getCRDGVRKindSubresource(unstructuredObj)
		resController.deleteResourceMapEntry(schema.GroupVersionResource{Group: group, Version: version, Resource: plural}, kind)
		return gvr
	default:
		klog.Errorf("object not Unstructured: type: %T val: %s\n", obj, obj)
		return schema.GroupVersionResource{}
	}
}

// Create a ListWatcher to iterate over resources for client side cache
// See kubernetes/pkg/controller/garbagecollector/graph_builder.go
func createListWatcher(dynamicClient dynamic.Interface, gvr schema.GroupVersionResource) *cache.ListWatch {
	return &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (k8sruntime.Object, error) {
			return dynamicClient.Resource(gvr).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return dynamicClient.Resource(gvr).Watch(options)
		},
	}
}
