package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// ServiceInfo holds information about a service from SDM status
type ServiceInfo struct {
	Name   string
	Region string
	SSHTag string
}

// NATSConnection holds information about a NATS connection
type NATSConnection struct {
	CID      string
	Name     string
	Server   string
	Cluster  string
	IP       string
	IPPort   string
	InMsgs   string
	OutMsgs  string
	InBytes  string
	OutBytes string
	Subs     string
}

// MatchedService represents a matched service with its NATS connection
type MatchedService struct {
	Service ServiceInfo
	Conn    NATSConnection
}

// UnmatchedIP represents an IP that couldn't be matched to a service
type UnmatchedIP struct {
	IP   string
	Conn NATSConnection
}

// VPC CIDR to Region mapping (common AWS VPC patterns)
var cidrToRegion = map[string]string{
	"172.30": "us-east-1",     // us-east-1 VPC
	"172.31": "us-west-1",     // us-west-1 VPC
	"172.21": "eu-central-1",  // eu-central-1 VPC
	"172.19": "ap-southeast-1", // ap-southeast-1 VPC
}

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s <sdmstatus.txt> <natsconn.txt>\n", os.Args[0])
		os.Exit(1)
	}

	sdmFile := os.Args[1]
	natsFile := os.Args[2]

	fmt.Printf("Parsing SDM status from: %s\n", sdmFile)
	ipToService := parseSDMStatus(sdmFile)
	fmt.Printf("Found %d services with IPs\n", len(ipToService))

	fmt.Printf("\nParsing NATS connections from: %s\n", natsFile)
	connections := parseNATSConnections(natsFile)
	fmt.Printf("Found %d connections\n", len(connections))

	// Group connections by cluster, then server
	matched := make(map[string]map[string][]MatchedService)
	unmatched := make(map[string]map[string][]UnmatchedIP)

	for _, conn := range connections {
		cluster := conn.Cluster
		server := conn.Server

		if _, ok := matched[cluster]; !ok {
			matched[cluster] = make(map[string][]MatchedService)
		}
		if _, ok := unmatched[cluster]; !ok {
			unmatched[cluster] = make(map[string][]UnmatchedIP)
		}

		if service, ok := ipToService[conn.IP]; ok {
			matched[cluster][server] = append(matched[cluster][server], MatchedService{
				Service: service,
				Conn:    conn,
			})
		} else {
			unmatched[cluster][server] = append(unmatched[cluster][server], UnmatchedIP{
				IP:   conn.IP,
				Conn: conn,
			})
		}
	}

	// Sort and print results
	printResults(matched, unmatched, ipToService)
}

func parseSDMStatus(filepath string) map[string]ServiceInfo {
	ipToService := make(map[string]ServiceInfo)

	file, err := os.Open(filepath)
	if err != nil {
		fmt.Printf("Error opening SDM status file: %v\n", err)
		return ipToService
	}
	defer file.Close()

	// Old format: ip=X.X.X.X in tags
	ipTagRegex := regexp.MustCompile(`ip=(\d+\.\d+\.\d+\.\d+)`)
	regionRegex := regexp.MustCompile(`region=([^,\s]+)`)
	sshRegex := regexp.MustCompile(`ssh=([^,\s]+)`)

	// New format: columns are name, IP, instance-id, tag
	// IP pattern to match standalone IP addresses
	ipColRegex := regexp.MustCompile(`^(\d+\.\d+\.\d+\.\d+)$`)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		var ip, serviceName, region, sshTag string

		// Try old format first (ip= in tags)
		if ipMatch := ipTagRegex.FindStringSubmatch(line); ipMatch != nil {
			ip = ipMatch[1]
			serviceName = parts[0]
			if regionMatch := regionRegex.FindStringSubmatch(line); regionMatch != nil {
				region = regionMatch[1]
			}
			if sshMatch := sshRegex.FindStringSubmatch(line); sshMatch != nil {
				sshTag = sshMatch[1]
			}
		} else if len(parts) >= 2 {
			// Try new format: name IP [instance-id] [tag]
			// Check if second column looks like an IP
			if ipColRegex.MatchString(parts[1]) {
				serviceName = parts[0]
				ip = parts[1]
				if len(parts) >= 4 {
					sshTag = parts[3] // The tag is in the 4th column
				}
				// Extract region from service name (e.g., xxx.us-east-1a -> us-east-1)
				if strings.Contains(serviceName, ".us-east-1") {
					region = "us-east-1"
				} else if strings.Contains(serviceName, ".us-west-1") {
					region = "us-west-1"
				} else if strings.Contains(serviceName, ".eu-central-1") {
					region = "eu-central-1"
				} else if strings.Contains(serviceName, ".ap-southeast-1") {
					region = "ap-southeast-1"
				}
			}
		}

		if ip != "" && serviceName != "" {
			ipToService[ip] = ServiceInfo{
				Name:   serviceName,
				Region: region,
				SSHTag: sshTag,
			}
		}
	}

	return ipToService
}

func parseNATSConnections(filepath string) []NATSConnection {
	var connections []NATSConnection

	file, err := os.Open(filepath)
	if err != nil {
		fmt.Printf("Error opening NATS connections file: %v\n", err)
		return connections
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		// Skip header and separator lines
		if !strings.Contains(line, "│") || strings.Contains(line, "CID") ||
			strings.Contains(line, "├") || strings.Contains(line, "╭") ||
			strings.Contains(line, "╰") || strings.Contains(line, "┼") ||
			strings.Contains(line, "TOTALS") {
			continue
		}

		// Parse the table row
		parts := strings.Split(line, "│")
		if len(parts) < 12 {
			continue
		}

		cid := strings.TrimSpace(parts[1])
		name := strings.TrimSpace(parts[2])
		server := strings.TrimSpace(parts[3])
		cluster := strings.TrimSpace(parts[4])
		ipPort := strings.TrimSpace(parts[5])
		// parts[6] = Account
		// parts[7] = Uptime
		inMsgs := strings.TrimSpace(parts[8])
		outMsgs := strings.TrimSpace(parts[9])
		inBytes := strings.TrimSpace(parts[10])
		outBytes := strings.TrimSpace(parts[11])
		subs := ""
		if len(parts) > 12 {
			subs = strings.TrimSpace(parts[12])
		}

		// Skip if no valid IP
		if ipPort == "" || !strings.Contains(ipPort, ":") {
			continue
		}

		// Extract IP without port
		ip := strings.Split(ipPort, ":")[0]

		connections = append(connections, NATSConnection{
			CID:      cid,
			Name:     name,
			Server:   server,
			Cluster:  cluster,
			IP:       ip,
			IPPort:   ipPort,
			InMsgs:   inMsgs,
			OutMsgs:  outMsgs,
			InBytes:  inBytes,
			OutBytes: outBytes,
			Subs:     subs,
		})
	}

	return connections
}

func extractRegionFromCluster(cluster string) string {
	clusterLower := strings.ToLower(cluster)
	switch {
	case strings.Contains(clusterLower, "usw1") || strings.Contains(clusterLower, "us-west"):
		return "us-west-1"
	case strings.Contains(clusterLower, "use1") || strings.Contains(clusterLower, "us-east"):
		return "us-east-1"
	case strings.Contains(clusterLower, "euc1") || strings.Contains(clusterLower, "eu-central"):
		return "eu-central-1"
	case strings.Contains(clusterLower, "apse1") || strings.Contains(clusterLower, "ap-southeast"):
		return "ap-southeast-1"
	default:
		return "unknown"
	}
}

func getRegionFromIP(ip string) string {
	parts := strings.Split(ip, ".")
	if len(parts) >= 2 {
		prefix := parts[0] + "." + parts[1]
		if region, ok := cidrToRegion[prefix]; ok {
			return region
		}
	}
	return "unknown"
}

func clusterSortKey(cluster string) int {
	clusterLower := strings.ToLower(cluster)
	region := extractRegionFromCluster(cluster)

	// Region priority: west=0, east=100, eu=200, ap=300
	regionPriority := 0
	switch region {
	case "us-west-1":
		regionPriority = 0
	case "us-east-1":
		regionPriority = 100
	case "eu-central-1":
		regionPriority = 200
	case "ap-southeast-1":
		regionPriority = 300
	default:
		regionPriority = 400
	}

	// Type priority: leaf=0, jetstream=10, read=20
	typePriority := 50
	if strings.Contains(clusterLower, "leaf") {
		typePriority = 0
	} else if strings.Contains(clusterLower, "jetstream") {
		typePriority = 10
	} else if strings.Contains(clusterLower, "read") {
		typePriority = 20
	}

	return regionPriority + typePriority
}

func printResults(matched map[string]map[string][]MatchedService, unmatched map[string]map[string][]UnmatchedIP, ipToService map[string]ServiceInfo) {
	// Get sorted cluster list
	var clusters []string
	for cluster := range matched {
		clusters = append(clusters, cluster)
	}
	for cluster := range unmatched {
		found := false
		for _, c := range clusters {
			if c == cluster {
				found = true
				break
			}
		}
		if !found {
			clusters = append(clusters, cluster)
		}
	}

	sort.Slice(clusters, func(i, j int) bool {
		return clusterSortKey(clusters[i]) < clusterSortKey(clusters[j])
	})

	fmt.Println("\n" + strings.Repeat("=", 120))
	fmt.Println("NATS CONNECTIONS BY CLUSTER AND SERVER")
	fmt.Println(strings.Repeat("=", 120))

	for _, cluster := range clusters {
		region := extractRegionFromCluster(cluster)
		fmt.Printf("\n%s\n", strings.Repeat("-", 100))
		fmt.Printf("CLUSTER: %s (Region: %s)\n", cluster, region)
		fmt.Printf("%s\n", strings.Repeat("-", 100))

		// Get sorted server list for this cluster
		var servers []string
		if m, ok := matched[cluster]; ok {
			for server := range m {
				servers = append(servers, server)
			}
		}
		if u, ok := unmatched[cluster]; ok {
			for server := range u {
				found := false
				for _, s := range servers {
					if s == server {
						found = true
						break
					}
				}
				if !found {
					servers = append(servers, server)
				}
			}
		}
		sort.Strings(servers)

		for _, server := range servers {
			fmt.Printf("\n  SERVER: %s\n", server)

			// Print matched services - just IP : name
			if m, ok := matched[cluster]; ok {
				if services, ok := m[server]; ok && len(services) > 0 {
					fmt.Printf("    MATCHED (%d IPs):\n", len(services))

					// Sort by IP for consistent output
					sort.Slice(services, func(i, j int) bool {
						return services[i].Conn.IP < services[j].Conn.IP
					})

					for _, s := range services {
						fmt.Printf("      %-18s : %s\n", s.Conn.IP, s.Service.Name)
					}
				}
			}

			// Print unmatched IPs (not found in SDM)
			if u, ok := unmatched[cluster]; ok {
				if ips, ok := u[server]; ok && len(ips) > 0 {
					// Get unique IPs
					uniqueIPs := make(map[string]bool)
					for _, ip := range ips {
						uniqueIPs[ip.IP] = true
					}

					fmt.Printf("    UNMATCHED (%d IPs) - not found in SDM:\n", len(uniqueIPs))
					var ipList []string
					for ip := range uniqueIPs {
						ipList = append(ipList, ip)
					}
					sort.Strings(ipList)
					for _, ip := range ipList {
						fmt.Printf("      %-18s : (unknown)\n", ip)
					}
				}
			}
		}
	}

	// Summary statistics
	fmt.Println("\n" + strings.Repeat("=", 120))
	fmt.Println("SUMMARY")
	fmt.Println(strings.Repeat("=", 120))

	totalMatched := 0
	totalUnmatched := 0
	uniqueServices := make(map[string]bool)

	for _, servers := range matched {
		for _, services := range servers {
			totalMatched += len(services)
			for _, s := range services {
				uniqueServices[s.Service.Name] = true
			}
		}
	}

	for _, servers := range unmatched {
		for _, ips := range servers {
			totalUnmatched += len(ips)
		}
	}

	fmt.Printf("Total matched connections (EC2):  %d\n", totalMatched)
	fmt.Printf("Total unmatched (likely K8s):     %d\n", totalUnmatched)
	fmt.Printf("Unique EC2 services identified:   %d\n", len(uniqueServices))

	if len(uniqueServices) > 0 {
		fmt.Println("\nAll identified EC2 services:")
		var serviceList []string
		for svc := range uniqueServices {
			serviceList = append(serviceList, svc)
		}
		sort.Strings(serviceList)
		for _, svc := range serviceList {
			fmt.Printf("  - %s\n", svc)
		}
	}

	// Cross-region connection analysis
	fmt.Println("\n" + strings.Repeat("=", 120))
	fmt.Println("CROSS-REGION CONNECTION ANALYSIS")
	fmt.Println(strings.Repeat("=", 120))

	crossRegion := make(map[string]map[string]int) // natsRegion -> sourceRegion -> count
	for cluster, servers := range unmatched {
		natsRegion := extractRegionFromCluster(cluster)
		if _, ok := crossRegion[natsRegion]; !ok {
			crossRegion[natsRegion] = make(map[string]int)
		}
		for _, ips := range servers {
			for _, ip := range ips {
				srcRegion := getRegionFromIP(ip.IP)
				crossRegion[natsRegion][srcRegion]++
			}
		}
	}

	var natsRegions []string
	for region := range crossRegion {
		natsRegions = append(natsRegions, region)
	}
	sort.Strings(natsRegions)

	for _, natsRegion := range natsRegions {
		sources := crossRegion[natsRegion]
		fmt.Printf("\nNATS in %s receives connections from:\n", natsRegion)
		var srcRegions []string
		for src := range sources {
			srcRegions = append(srcRegions, src)
		}
		sort.Strings(srcRegions)
		for _, src := range srcRegions {
			count := sources[src]
			crossNote := ""
			if src != natsRegion && src != "unknown" {
				crossNote = " [CROSS-REGION]"
			}
			fmt.Printf("  - %-20s: %d connections%s\n", src, count, crossNote)
		}
	}

	// Output all unique unmatched IPs for kubectl lookup
	fmt.Println("\n" + strings.Repeat("=", 120))
	fmt.Println("ALL UNIQUE IPs (for kubectl lookup)")
	fmt.Println(strings.Repeat("=", 120))
	fmt.Println("Run: kubectl get pods -o wide -A | grep -E '<IP1>|<IP2>|...'")
	fmt.Println()

	allUniqueIPs := make(map[string]bool)
	for _, servers := range unmatched {
		for _, ips := range servers {
			for _, ip := range ips {
				allUniqueIPs[ip.IP] = true
			}
		}
	}

	var ipList []string
	for ip := range allUniqueIPs {
		ipList = append(ipList, ip)
	}
	sort.Strings(ipList)

	// Group by region for organized output
	ipsByRegion := make(map[string][]string)
	for _, ip := range ipList {
		region := getRegionFromIP(ip)
		ipsByRegion[region] = append(ipsByRegion[region], ip)
	}

	var regionList []string
	for region := range ipsByRegion {
		regionList = append(regionList, region)
	}
	sort.Strings(regionList)

	for _, region := range regionList {
		ips := ipsByRegion[region]
		fmt.Printf("\n%s (%d IPs):\n", region, len(ips))
		// Print in a grep-friendly format
		fmt.Printf("  grep pattern: %s\n", strings.Join(ips, "|"))
	}
}
