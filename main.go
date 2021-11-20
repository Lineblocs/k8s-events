package main
import (
	"log"
	"os"
	"fmt"
	"database/sql"
	"context"
	"time"
	"flag"
	"errors"
	"net/http"
		"k8s.io/client-go/tools/clientcmd"
	"encoding/json"
		"path/filepath"
			"github.com/gorilla/mux"
	"github.com/gorilla/handlers"
	_ "github.com/go-sql-driver/mysql"
	lineblocs "bitbucket.org/infinitet3ch/lineblocs-go-helpers"
	"k8s.io/client-go/kubernetes"
    v1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/homedir"
)

var db* sql.DB;

type PodMetadata struct {
	generatedName string `json:"generatedName"`
	labels map[string]string `json:"labels"`
	name string `json:"name"`
	namespace string `json:"name"`
}
type PodResource struct {
	kind string `json:"kind"`
	metadata PodMetadata `json:"metadata"`
	hostIp string `json:"hostIP"`
	podIp string `json:"potIP"`
	phase string `json:"phase"`
	qosClass string `json:"qosClass"`
	startTime string `json:"startTime"`
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
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		return nil, err
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return clientset, nil
}
func GetContainerIP(clientset kubernetes.Interface, podName, namespace string) (string, error) {
    // creates the in-cluster config
    IP := ""
    for {
        if IP != "" {
            break
        } else {
            log.Printf("No IP for now.\n")
        }

			ctx := context.Background()
        pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
        if err != nil {
			return "", err
        }
		ctx2 := context.Background()
        for _, pod := range pods.Items {
			if podName == pod.Name {
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

        log.Printf("Waits...\n")
        time.Sleep(1 * time.Second)
    }
    return "", errors.New("Could not fetch IP")
}

func processEvent(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-Type", "application/json")
  var podResource PodResource

   err := json.NewDecoder(r.Body).Decode(&podResource)
 if err != nil {
		fmt.Println("Could not process json. error: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return 
	}
	clientset, err := createK8sConfig()
 if err != nil {
		fmt.Println("Could not get k8s config. error: " + err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return 
	}
	region := os.Getenv("LINEBLOCS_REGION")
	component := podResource.metadata.labels["component"]
	name := podResource.metadata.name
	ns := podResource.metadata.namespace
	if component == "asterisk" {
		switch phase := podResource.phase; phase {
			case "Running": // upscale
				var id string
				row:=db.QueryRow("select id from media_servers where k8s_pod_id = ?", )
				err := row.Scan(&id)
				if ( err == sql.ErrNoRows ) {  //create conference
					// create it
					stmt, err := db.Prepare("INSERT INTO media_servers (`k8s_pod_id`, `name`, `ip_address`, `ip_address_range`, `private_ip_address`, `private_ip_range`) VALUES (?, ?, ?, ?, ?, ?)")


					if err != nil {
						fmt.Println("error occured: " + err.Error())
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					// check media server IP
					ip, err := GetContainerIP( clientset, name, ns )
					if err != nil {
						fmt.Println("error occured: " + err.Error())
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					ipRange:="/32"
					res, err := stmt.Exec( name, ip, ipRange, ip, ipRange )
					if err != nil {
						fmt.Println("error occured: " + err.Error())
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					mediaServerId, err := res.LastInsertId()
					if err != nil {
						fmt.Println("error occured: " + err.Error())
						w.WriteHeader(http.StatusInternalServerError)
						return
					}

					// add new media server to all routers
					results, err := db.Query("SELECT id FROM sip_routes;")
					if err != nil {
						fmt.Println("error occured: " + err.Error())
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					for results.Next() {
						var id string
						err = results.Scan(&id);
						if err != nil {
							fmt.Println("error occured: " + err.Error())
							w.WriteHeader(http.StatusInternalServerError)
							return
						}

						stmt, err := db.Prepare("INSERT INTO sip_routers_media_servers (`router_id`, `media_server_id`) VALUES (?,?)")
						if err != nil {
							fmt.Println("error occured: " + err.Error())
							w.WriteHeader(http.StatusInternalServerError)
							return
						}
						defer stmt.Close()
						_, err =stmt.Exec( id,mediaServerId )
						if err != nil {
							fmt.Println("error occured: " + err.Error())
							w.WriteHeader(http.StatusInternalServerError)
							return
						}
					}
					defer stmt.Close()
				}
				if ( err != nil ) {  //another error
					fmt.Println("error occured: " + err.Error())
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			case "Terminated": // downscale
				var id string
				row:=db.QueryRow("select id from media_servers where k8s_pod_id = ?", )
				err := row.Scan(&id)
				if ( err == sql.ErrNoRows ) {  //create conference
					// create it
					fmt.Println("error occured: " + err.Error())
					w.WriteHeader(http.StatusInternalServerError)
				}
				stmt, err := db.Prepare("DELETE FROM media_servers WHERE `k8s_pod_id` =?")


				if err != nil {
					fmt.Println("error occured: " + err.Error())
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				defer stmt.Close()
		}
	} else if component == "opensips" {
		switch phase := podResource.phase; phase {
			case "Running": // upscale
				var id string
				row:=db.QueryRow("select id from sip_routers where k8s_pod_id = ?", )
				err := row.Scan(&id)
				if ( err == sql.ErrNoRows ) {  //create conference
					// create it
					stmt, err := db.Prepare("INSERT INTO sip_routers (`k8s_pod_id`, `name`, `ip_address`, `ip_address_range`, `private_ip_address`, `private_ip_range`, `region`) VALUES (?, ?, ?, ?, ?, ?)")


					if err != nil {
						fmt.Println("error occured: " + err.Error())
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					defer stmt.Close()
					// check router IP
					ip, err := GetContainerIP( clientset, name, ns )
					if err != nil {
						fmt.Println("error occured: " + err.Error())
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					ipRange:="/32"
					_, err = stmt.Exec( name, ip, ipRange, ip, ipRange, region )
					if err != nil {
						fmt.Println("error occured: " + err.Error())
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
				}
			case "Terminated": // downscale
				var id string
				row:=db.QueryRow("select id from sip_routers where k8s_pod_id = ?", )
				err := row.Scan(&id)
				if ( err == sql.ErrNoRows ) {  //create conference
					// doesnt exist
					fmt.Println("error occured: " + err.Error())
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				stmt, err := db.Prepare("DELETE FROM sip_routers WHERE `k8s_pod_id` =?")


				if err != nil {
					fmt.Println("error occured: " + err.Error())
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				defer stmt.Close()
		}
	}
}
func main() {
	log.Print("connecting to DB...")
	var err error
	db, err =lineblocs.CreateDBConn()
	if err != nil {
		panic(err)
	}
    r := mux.NewRouter()
    // Routes consist of a path and a handler function.
	r.HandleFunc("/processEvent", processEvent).Methods("POST");
	// Bind to a port and pass our router in
	loggedRouter := handlers.LoggingHandler(os.Stdout, r)
	log.Print("Starting server...")
    log.Fatal(http.ListenAndServe(":80", loggedRouter))
}