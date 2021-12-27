package main
import (
	"log"
	"os"
	"fmt"
	"database/sql"
	"context"
	"time"
	"errors"
	"net/http"
	"net/http/httputil"
	"k8s.io/client-go/rest"
		_ "k8s.io/client-go/tools/clientcmd"
	"encoding/json"
			"github.com/gorilla/mux"
	"github.com/gorilla/handlers"
	_ "github.com/go-sql-driver/mysql"
	lineblocs "bitbucket.org/infinitet3ch/lineblocs-go-helpers"
	"k8s.io/client-go/kubernetes"
    v1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var db* sql.DB;

type PodMetadata struct {
	GenerateName string `json:"generateName"`
	Labels map[string]string `json:"labels"`
	Name string `json:"name"`
	Namespace string `json:"namespace"`
}
type PodStatus struct {
	Phase string `json:"phase"`
}
type PodResource struct {
	Kind string `json:"kind"`
	Metadata PodMetadata `json:"metadata"`
	HostIp string `json:"hostIP"`
	PodIp string `json:"potIP"`
	Status PodStatus `json:"status"`
	Phase string `json:"phase"`
	QosClass string `json:"qosClass"`
	StartTime string `json:"startTime"`
}
type Event struct {
	Pod PodResource `json:"pod"`
}




func ListNodes(clientset kubernetes.Interface) ([]v1.Node, error) {
    nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})

    if err != nil {
        return nil, err
    }

    return nodes.Items, nil
}

func GetNodeByName(clientset kubernetes.Interface, name string, namespace string) (*v1.Node, error) {
 	nodes, err := ListNodes(clientset)
    if err != nil {
        return nil, err
    }
	for _, item := range nodes {
		if item.Name == name {
			return &item, nil
		}
	}
	return nil, errors.New("No node found.")
}

func createK8sConfig() (kubernetes.Interface, error) {
	/*
	var kubeconfig string
	kubeconfig= "/root/.kube/config"
	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	*/
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil,err
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return clientset, nil
}

func createDNSRecords(name, ip, region string) (error) {
	return nil
}

func removeDNSRecords(name, ip, region string) (error) {
	return nil
}
func GetContainerIP(clientset kubernetes.Interface, podName, namespace string) (string, error) {
    // creates the in-cluster config
	fmt.Println("searching in NS: " + namespace)
    IP := ""
    for {
        if IP != "" {
            break
        } else {
            //log.Printf("No IP for now.\n")
        }

			ctx := context.Background()
        pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
        if err != nil {
			return "", err
        }
		ctx2 := context.Background()
		found :=false
        for _, pod := range pods.Items {
			fmt.Printf("matching pod %s to %s\r\n", podName, pod.Name)
			if podName == pod.Name {
				found = true
				pod, err := clientset.CoreV1().Pods(namespace).Get(ctx2, pod.Name, metav1.GetOptions{})
				if err != nil {
					return "", err
				}
				_ = pod.Status.PodIP
				nodeName := pod.Spec.NodeName
				node, err := GetNodeByName(clientset, nodeName, namespace)
				if err != nil {
					return "", err
				}
				for _, item := range node.Status.Addresses {
					if item.Type == "ExternalIP" {
						return item.Address, nil
					}
				}
			}
        }
		if !found {
			return "", errors.New("pod does not exist...")
		}

        //log.Printf("Waits...\n")
        time.Sleep(1 * time.Second)
    }
    return "", errors.New("Could not fetch IP")
}

// formatRequest generates ascii representation of a request
func formatRequest(r *http.Request) (string, error) {
	res, err := httputil.DumpRequest(r, true)  
	if err != nil {  
		log.Fatal(err)  
		return  "", err
	}  
	 return string(res), nil

}

func getMediaServers() ([]string, error) {

	pods:=make([]string,0)
	results, err := db.Query("SELECT k8s_pod_id FROM media_servers")
	if err != nil {
		return pods, err
	}
	defer results.Close()

	for results.Next() {
		var podId string
		err := results.Scan(&podId)
		if err != nil {
			return pods,err
		}
		pods=append(pods,podId)
	}
	return pods,nil
}

func synchronizePodWithDatabase(clientset kubernetes.Interface, component, name, ns, phase string) (error) {
	region := os.Getenv("LINEBLOCS_REGION")
	if component == "asterisk" {
		switch ; phase {
			case "Running": // upscale
				var id string
				row:=db.QueryRow("select id from media_servers where k8s_pod_id = ?", name)
				err := row.Scan(&id)
				if ( err == sql.ErrNoRows ) {  //create conference
					// create it
					fmt.Println("creating media server " + name)
					stmt, err := db.Prepare("INSERT INTO media_servers (`k8s_pod_id`, `name`, `ip_address`, `ip_address_range`, `private_ip_address`, `private_ip_address_range`, `created_at`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")


					if err != nil {
						return err
					}
					// check media server IP
					ip, err := GetContainerIP( clientset, name, ns )
					if err != nil {
						return err
					}

					// add 5160 for media servers
					ip = ip + ":5160"
					ipRange:="/32"
					now :=time.Now()
					res, err := stmt.Exec( name,name, ip, ipRange, ip, ipRange, now,now )
					if err != nil {
						return err
					}
					mediaServerId, err := res.LastInsertId()
					if err != nil {
						return err
					}

					// add new media server to routers located in region
					results, err := db.Query("SELECT id FROM sip_routers WHERE region = ?;", region)
					if err != nil {
						return err
					}
					for results.Next() {
						var id string
						err = results.Scan(&id);
						if err != nil {
							return err
						}

						stmt, err := db.Prepare("INSERT INTO sip_routers_media_servers (`router_id`, `server_id`, `created_at`, `updated_at`) VALUES (?,?,?,?)")
						if err != nil {
							return err
						}
						defer stmt.Close()
						_, err =stmt.Exec( id,mediaServerId,now,now )
						if err != nil {
							return err
						}
					}
					defer stmt.Close()
				}
				if ( err != nil ) {  //another error
					return err
				}
			case "Deleted","Terminating","CrashLoopBackOff": // downscale
				var id string
				row:=db.QueryRow("select id from media_servers where k8s_pod_id = ?", )
				err := row.Scan(&id)
				if ( err == sql.ErrNoRows ) {  //create conference
					// create it
					return err
				}
				fmt.Println("deleting media server " + name)
				stmt, err := db.Prepare("DELETE FROM media_servers WHERE `k8s_pod_id` =?")


				if err != nil {
					return err
				}
				_, err =stmt.Exec( name )
				if err != nil {
					return err
				}

				defer stmt.Close()
		}
	} else if component == "rtpproxy" {
		switch ; phase {
			case "Running": // upscale
				var id string
				row:=db.QueryRow("select id from rtpproxy_sockets where k8s_pod_id = ?", name)
				err := row.Scan(&id)
				if ( err == sql.ErrNoRows ) {  //create conference
					// create it
					fmt.Println("creating rtp proxy " + name)
					stmt, err := db.Prepare("INSERT INTO rtpproxy_sockets (`k8s_pod_id`, `rtpproxy_sock`, `set_id`, `created_at`, `updated_at`) VALUES (?, ?, ?, ?, ?)")


					if err != nil {
						return err
					}
					defer stmt.Close()
					// check media server IP
					ip, err := GetContainerIP( clientset, name, ns )
					if err != nil {
						return err
					}

					// add 5160 for media servers
					sock := "udp:" + ip + ":7722"
					setId := "1"
					now :=time.Now()
					_, err = stmt.Exec( name,sock,setId, now,now )
					if err != nil {
						return err
					}
				}
				if ( err != nil ) {  //another error
					return err
				}
			case "Deleted","Terminating","CrashLoopBackOff": // downscale
				var id string
				row:=db.QueryRow("select id from rtpproxy_sockets where k8s_pod_id = ?", )
				err := row.Scan(&id)
				if ( err == sql.ErrNoRows ) {  //create conference
					// create it
					return err
				}
				fmt.Println("deleting rtp proxy " + name)
				stmt, err := db.Prepare("DELETE FROM rtpproxy_sockets WHERE `k8s_pod_id` =?")


				if err != nil {
					return err
				}
				_, err =stmt.Exec( name )
				if err != nil {
					return err
				}

				defer stmt.Close()
		}
	} else if component == "opensips" {
		switch ; phase {
			case "Running": // upscale
				var id string
				row:=db.QueryRow("select id from sip_routers where k8s_pod_id = ?", )
				err := row.Scan(&id)
				if ( err == sql.ErrNoRows ) {  //create conference
					// create it
					fmt.Println("creating SIP router " + name)
					now :=time.Now()
					stmt, err := db.Prepare("INSERT INTO sip_routers (`k8s_pod_id`, `name`, `ip_address`, `ip_address_range`, `private_ip_address`, `private_ip_range`, `region`, `created_at`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")


					if err != nil {
						return err
					}
					defer stmt.Close()
					// check router IP
					ip, err := GetContainerIP( clientset, name, ns )
					if err != nil {
						return err
					}
					ipRange:="/32"
					_, err = stmt.Exec( name,name, ip, ipRange, ip, ipRange, region, now,now )
					if err != nil {
						return err
					}
					err = createDNSRecords(name, ip, region)
					if err != nil {
						return err
					}
				}
			case "Deleted","Terminating","CrashLoopBackOff": // downscale
				var id string
				var ipAddr string
				row:=db.QueryRow("select id, ip_address from sip_routers where k8s_pod_id = ?", )
				err := row.Scan(&id, &ipAddr)
				if ( err == sql.ErrNoRows ) {  //create conference
					// doesnt exist
					return err
				}
				fmt.Println("deleting SIP router " + name)
				stmt, err := db.Prepare("DELETE FROM sip_routers WHERE `k8s_pod_id` =?")


				if err != nil {
					return err
				}
				defer stmt.Close()
				_, err =stmt.Exec( name )
				if err != nil {
					return err
				}


				err = removeDNSRecords(name, ipAddr, region)
				if err != nil {
					return err
				}
		}
	}
	return nil
}
func processEvent(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-Type", "application/json")
  fmt.Println("received request..")
  var event Event
  var podResource PodResource
  var err error
  _, err = formatRequest(r)
 if err != nil {
		fmt.Println("Could not process json. error: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return 
	}
   err = json.NewDecoder(r.Body).Decode(&event)
 if err != nil {
		fmt.Println("Could not process json. error: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return 
	}

	data, err := json.Marshal(podResource)

	fmt.Println(string(data))
	podResource = event.Pod
	clientset, err := createK8sConfig()
 if err != nil {
		fmt.Println("Could not get k8s config. error: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return 
	}
	//region := os.Getenv("LINEBLOCS_REGION")
	component := podResource.Metadata.Labels["component"]
	//hash := podResource.Metadata.Labels["pod-template-hash"]
	//name := podResource.Metadata.GenerateName + "-" + hash
	name := podResource.Metadata.Name
	ns := podResource.Metadata.Namespace
	phase := podResource.Status.Phase

	if ns != "voip" {
		fmt.Println("namespace is not voip, skipping...\r\n")
		return
	}

	//fmt.Println( "incoming request: " )
	//fmt.Println( txt )
	fmt.Println("component is: " + component)
	fmt.Println("phase is: " + phase)

	err = synchronizePodWithDatabase(clientset, component, name, ns, phase)
	if err != nil {
		fmt.Println("error occured: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}


func pollForPodChanges() {

	clientset, err := createK8sConfig()
 	if err != nil {
		 fmt.Println("error: " + err.Error())
		return 
	}
	for ;; {
		ctx :=context.Background()
		namespace:="voip"
		pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			fmt.Println("error: " + err.Error())
			continue
		}
		for _, podResource := range pods.Items {
			fmt.Println("pod: " + podResource.Name)
			component := podResource.ObjectMeta.Labels["component"]
			fmt.Println("component: " + component)
			name := podResource.Name
			phase := string( podResource.Status.Phase )
			fmt.Println("phase: " + phase)
			err = synchronizePodWithDatabase(clientset, component, name, "voip", phase)
			if err != nil {
				fmt.Println("error: " + err.Error())
				continue
			}
		}
		servers,err:=getMediaServers()
		if err != nil {
			fmt.Println("error: " + err.Error())
			continue
		}

		for _, podId := range servers {
			found:=false
			for _, podResource := range pods.Items {
				name := podResource.Name
				if name==podId {
					found=true
					phase := string(podResource.Status.Phase)

					component:="asterisk"
					err = synchronizePodWithDatabase(clientset, component, podId, "voip", phase)
					if err != nil {
						fmt.Println("error: " + err.Error())
						continue
					}

				}
			}
			if !found {
				component:="asterisk"
				err = synchronizePodWithDatabase(clientset, component, podId, "voip", "Deleted")
				if err != nil {
					fmt.Println("error: " + err.Error())
					continue
				}

			}
		}

		time.Sleep( time.Duration( time.Second * 5 ))
	}
}
func main() {
	log.Print("connecting to DB...")
	var err error
	db, err =lineblocs.CreateDBConn()
	if err != nil {
		panic(err)
	}
	go pollForPodChanges()
    r := mux.NewRouter()
    // Routes consist of a path and a handler function.
	r.HandleFunc("/processEvent", processEvent).Methods("POST");
	// Bind to a port and pass our router in
	loggedRouter := handlers.LoggingHandler(os.Stdout, r)
	log.Print("Starting server...")
    log.Fatal(http.ListenAndServe(":8000", loggedRouter))
}