variable "node_count" {
  default = 5
}

job "capukv" {
  region      = "global"
  type        = "service"
  datacenters = ["monke-ca-toronto-01"]

  update {
    max_parallel     = var.node_count
    healthy_deadline = "5m"
    auto_revert      = true
  }

  group "capukv" {
    count = var.node_count

    network {
      port "front_port" {
        host_network = "front"
      }
      port "back_port" {
        host_network = "back"
      }
    }

    task "capukv" {
      driver = "docker"

      restart {
        attempts = 3
        delay    = "5s"
      }

      config {
        force_pull = true
        image = "git.monke.ca/monke/capukv:dev"
        ports = ["front_port", "back_port"]

        args = [
          "--dir", "/data/capukv",
          "--api-addr", "0.0.0.0:${NOMAD_PORT_front_port}",
          "--redirect-uri", "http://${NOMAD_ADDR_front_port}",
          "--raft-addr", "0.0.0.0:${NOMAD_PORT_back_port}",
          "--dns", "capukv-raft.service.consul.",
          "--expect", "${var.node_count}",
        ]
      }

      resources {
        cpu    = 150
        memory = 128
      }

      service {
        name = "capukv-raft"
        port = "back_port"
      }

      service {
        name = "capukv"
        port = "front_port"

        /*
        check {
          type     = " http "
          path     = " / health-check "
          interval = " 5 s "
          timeout  = " 2 s "
        }
        */

        tags = [
          "traefik.enable=true",
          "traefik.http.routers.capukv.rule=Host(`capukv.testnet`)",
          "traefik.http.routers.capukv.entrypoints=web",
        ]
      }
    }
  }

}
