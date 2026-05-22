# 

# EKNSoft ERP — Arquitetura Técnica Aprofundada
> Versão 2.0 — Multiempresa + Softhouse | PostgreSQL + NestJS + Next.js 15

---

## Sumário

1. [Visão Geral e Estratégia de Evolução](#1-visão-geral-e-estratégia-de-evolução)
2. [Bounded Contexts e Estrutura de Pastas](#2-bounded-contexts-e-estrutura-de-pastas)
3. [Estratégia Multitenancy — 3 Camadas de Isolamento](#3-estratégia-multitenancy--3-camadas-de-isolamento)
4. [Fluxo de Autenticação e JWT](#4-fluxo-de-autenticação-e-jwt)
5. [Modelagem do Banco de Dados — Prisma Schema Completo](#5-modelagem-do-banco-de-dados--prisma-schema-completo)
6. [RBAC Avançado — Permissões e Roles](#6-rbac-avançado--permissões-e-roles)
7. [Middlewares, Guards e Decorators](#7-middlewares-guards-e-decorators)
8. [Estratégia de Índices e Performance](#8-estratégia-de-índices-e-performance)
9. [Auditoria e Rastreabilidade](#9-auditoria-e-rastreabilidade)
10. [Estratégia de Cache Redis](#10-estratégia-de-cache-redis)
11. [Filas com RabbitMQ](#11-filas-com-rabbitmq)
12. [Docker Compose e Deploy](#12-docker-compose-e-deploy)
13. [Seed Inicial](#13-seed-inicial)
14. [Plano de Escalabilidade](#14-plano-de-escalabilidade)

---

## 1. Visão Geral e Estratégia de Evolução

### 1.1 Por que Modular Monolith First?

Microserviços desde o dia 1 trazem complexidade operacional desproporcional ao tamanho inicial. A estratégia adotada é a evolução gradual:

```
Fase 1 — Modular Monolith        (0 → 500 tenants)
  ├─ Único deployment NestJS
  ├─ Módulos com contratos bem definidos e zero vazamento entre eles
  ├─ Comunicação interna via EventEmitter2 (substitui RabbitMQ nos testes)
  └─ PostgreSQL único + Redis

Fase 2 — Service Extraction      (500 → 2.000 tenants)
  ├─ Módulos Fiscal e Financeiro extraídos como serviços
  ├─ RabbitMQ como message broker real
  ├─ API Gateway (Kong ou Traefik)
  └─ Read replicas por módulo de alta leitura

Fase 3 — Microservices           (> 2.000 tenants)
  ├─ Serviços independentes por Bounded Context
  ├─ CQRS com bancos separados (write: PostgreSQL, read: Redis/ElasticSearch)
  ├─ Event Sourcing para módulos críticos (Financeiro, Audit)
  └─ Kubernetes com HPA
```

### 1.2 Princípios Arquiteturais

- **Clean Architecture**: Domain → Application → Infrastructure → Presentation
- **SOLID** aplicado em cada módulo
- **DDD**: Entities, Value Objects, Aggregates, Domain Events, Repositories
- **CQRS**: Comandos (escrita) separados de Queries (leitura) desde o início
- **Fail-fast multitenancy**: qualquer query sem `softhouse_id` é rejeitada

---

## 2. Bounded Contexts e Estrutura de Pastas

### 2.1 Bounded Contexts

| Contexto            | Responsabilidade                                                |
|---------------------|-----------------------------------------------------------------|
| `identity-access`   | Auth, usuários, roles, permissões, sessões, MFA                 |
| `tenancy`           | Softhouses, empresas, planos, assinaturas, módulos habilitados  |
| `financial`         | Contas, lançamentos, fluxo de caixa, conciliação bancária       |
| `inventory`         | Produtos, categorias, estoque, movimentações, inventário        |
| `sales`             | Pedidos, orçamentos, PDV, comissões, NF                         |
| `fiscal`            | NFe, NFCe, SAT, SPED, certificados digitais                     |
| `crm`               | Leads, pipeline, oportunidades, atividades                      |
| `reporting`         | Dashboards, KPIs, exportação PDF/Excel                          |
| `notifications`     | Email, push, webhooks, alertas                                  |

### 2.2 Estrutura de Pastas Backend

```
apps/api/src/
├── modules/
│   ├── auth/
│   │   ├── commands/
│   │   │   ├── login.command.ts
│   │   │   ├── logout.command.ts
│   │   │   └── refresh-token.command.ts
│   │   ├── queries/
│   │   │   └── get-session.query.ts
│   │   ├── domain/
│   │   │   ├── entities/user.entity.ts
│   │   │   ├── value-objects/email.vo.ts
│   │   │   ├── value-objects/password.vo.ts
│   │   │   └── events/user-logged-in.event.ts
│   │   ├── application/
│   │   │   ├── use-cases/login.usecase.ts
│   │   │   └── dtos/login.dto.ts
│   │   ├── infrastructure/
│   │   │   ├── repositories/user.repository.ts
│   │   │   └── services/jwt.service.ts
│   │   └── presentation/
│   │       ├── auth.controller.ts
│   │       └── auth.module.ts
│   │
│   ├── tenancy/
│   ├── financial/
│   ├── inventory/
│   ├── sales/
│   ├── fiscal/
│   ├── crm/
│   └── reporting/
│
├── shared/
│   ├── domain/
│   │   ├── base.entity.ts          # ID, created_at, updated_at, deleted_at
│   │   ├── tenant.entity.ts        # + softhouse_id, empresa_id
│   │   └── aggregate-root.ts
│   ├── infrastructure/
│   │   ├── database/
│   │   │   ├── prisma.service.ts
│   │   │   ├── tenant-middleware.ts # Inject automático softhouse_id/empresa_id
│   │   │   └── rls.service.ts       # SET LOCAL app.softhouse_id
│   │   ├── cache/
│   │   │   └── redis.service.ts
│   │   ├── queue/
│   │   │   └── rabbitmq.service.ts
│   │   └── tenant/
│   │       └── tenant-context.service.ts  # AsyncLocalStorage
│   ├── middleware/
│   │   ├── tenant.middleware.ts
│   │   └── rate-limit.middleware.ts
│   ├── guards/
│   │   ├── jwt-auth.guard.ts
│   │   ├── roles.guard.ts
│   │   └── permissions.guard.ts
│   ├── decorators/
│   │   ├── current-user.decorator.ts
│   │   ├── require-permissions.decorator.ts
│   │   └── tenant.decorator.ts
│   └── interceptors/
│       ├── audit.interceptor.ts
│       └── transform.interceptor.ts
│
├── prisma/
│   ├── schema.prisma
│   ├── migrations/
│   └── seed/
│       ├── index.ts
│       ├── super-admin.seed.ts
│       ├── plans.seed.ts
│       └── softhouse-demo.seed.ts
└── main.ts
```

### 2.3 Estrutura de Pastas Frontend

```
apps/web/src/
├── app/
│   ├── (auth)/
│   │   ├── login/page.tsx
│   │   └── recuperar-senha/page.tsx
│   ├── (master)/            # Super Admin
│   │   ├── layout.tsx
│   │   ├── dashboard/page.tsx
│   │   ├── softhouses/
│   │   ├── planos/
│   │   └── logs/
│   ├── (softhouse)/         # Painel Softhouse
│   │   ├── layout.tsx
│   │   ├── dashboard/page.tsx
│   │   ├── empresas/
│   │   └── financeiro/
│   └── (erp)/               # Painel Empresa
│       ├── layout.tsx
│       ├── dashboard/page.tsx
│       ├── vendas/
│       ├── financeiro/
│       ├── estoque/
│       ├── fiscal/
│       └── crm/
├── components/
│   ├── ui/                  # ShadCN components
│   ├── layout/
│   └── shared/
├── hooks/
│   ├── use-permissions.ts
│   └── use-tenant.ts
├── stores/
│   ├── auth.store.ts        # Zustand
│   └── tenant.store.ts
└── lib/
    ├── api.ts               # Axios instance com interceptors
    └── permissions.ts
```

---

## 3. Estratégia Multitenancy — 3 Camadas de Isolamento

> Princípio: **Defesa em profundidade** — se uma camada falhar, as outras garantem o isolamento.

### Camada 1 — JWT Claims (Request Layer)

O token JWT carrega `softhouse_id` e `empresa_id`. O `JwtAuthGuard` extrai e valida esses dados antes de qualquer handler ser executado.

```typescript
// modules/auth/infrastructure/services/jwt.service.ts
export interface JwtPayload {
  sub: string;            // user UUID
  jti: string;            // session UUID (para revogação)
  type: 'access' | 'refresh';
  tenant: {
    softhouse_id: string;
    empresa_id: string | null;    // null para usuários de Softhouse
    softhouse_slug: string;
  };
  role: UserRole;
  permissions: string[];  // ex: ['financeiro:criar', 'estoque:visualizar']
  iat: number;
  exp: number;
}

// Estrutura real do token decodificado:
// {
//   sub: "550e8400-e29b-41d4-a716-446655440000",
//   jti: "7c9e6679-7425-40de-944b-e07fc1f90ae7",
//   type: "access",
//   tenant: {
//     softhouse_id: "d3b07384-d9a9-4c5b-9a00-3edb6f68f7e9",
//     empresa_id: "a87ff679-a2f3-471d-9b79-5f2c8b05c254",
//     softhouse_slug: "techsolutions"
//   },
//   role: "EMPRESA_FINANCEIRO",
//   permissions: ["financeiro:visualizar", "financeiro:criar", "relatorios:exportar:pdf"],
//   iat: 1700000000,
//   exp: 1700003600
// }
```

### Camada 2 — Prisma Middleware (Application Layer)

Toda query ao banco é interceptada e tem `softhouse_id` injetado automaticamente:

```typescript
// shared/infrastructure/database/tenant-middleware.ts
import { Prisma } from '@prisma/client';
import { tenantStorage } from '../tenant/tenant-context.service';

// Tabelas que pertencem à plataforma (sem tenant)
const PLATFORM_TABLES = new Set([
  'SuperAdmin', 'Softhouse', 'Plan', 'Subscription', 'AuditLog'
]);

// Tabelas que pertencem à softhouse mas não a uma empresa específica
const SOFTHOUSE_TABLES = new Set([
  'Empresa', 'SoftouseUser', 'SofthouseConfig', 'EmpresaModulo'
]);

export function applyTenantMiddleware(prisma: PrismaClient) {
  prisma.$use(async (params: Prisma.MiddlewareParams, next) => {
    const ctx = tenantStorage.getStore();

    // Sem contexto ou tabela de plataforma: passa direto
    if (!ctx || PLATFORM_TABLES.has(params.model ?? '')) {
      return next(params);
    }

    const isSoftouseTable = SOFTHOUSE_TABLES.has(params.model ?? '');

    const tenantFilter = {
      softhouse_id: ctx.softouseId,
      ...(!isSoftouseTable && ctx.empresaId ? { empresa_id: ctx.empresaId } : {}),
    };

    // ─── Leitura ────────────────────────────────────────────────────────────
    if (['findMany', 'findFirst', 'findFirstOrThrow', 'count', 'aggregate', 'groupBy'].includes(params.action)) {
      params.args = params.args ?? {};
      params.args.where = { ...params.args.where, ...tenantFilter };
    }

    // ─── Criação ────────────────────────────────────────────────────────────
    if (params.action === 'create') {
      params.args.data = { ...params.args.data, ...tenantFilter };
    }

    if (params.action === 'createMany') {
      params.args.data = params.args.data.map((d: any) => ({ ...d, ...tenantFilter }));
    }

    // ─── Atualização/Deleção em massa ────────────────────────────────────────
    if (['updateMany', 'deleteMany'].includes(params.action)) {
      params.args.where = { ...params.args.where, ...tenantFilter };
    }

    // ─── Update/Delete por ID — adiciona where para validar posse ───────────
    if (['update', 'delete'].includes(params.action)) {
      // Transforma em operação "where" para garantir que o tenant é dono do registro
      if (params.args.where?.id) {
        params.args.where = { ...params.args.where, ...tenantFilter };
      }
    }

    return next(params);
  });
}
```

### Camada 3 — PostgreSQL Row Level Security (Database Layer)

RLS é o último escudo: mesmo que o middleware falhe ou um DBA conecte diretamente, os dados ficam protegidos.

```sql
-- ── Habilitar RLS nas tabelas sensíveis ──────────────────────────────────────
ALTER TABLE empresas              ENABLE ROW LEVEL SECURITY;
ALTER TABLE clientes              ENABLE ROW LEVEL SECURITY;
ALTER TABLE financeiro_contas     ENABLE ROW LEVEL SECURITY;
ALTER TABLE produtos              ENABLE ROW LEVEL SECURITY;
ALTER TABLE pedidos               ENABLE ROW LEVEL SECURITY;
ALTER TABLE nfe                   ENABLE ROW LEVEL SECURITY;
ALTER TABLE crm_leads             ENABLE ROW LEVEL SECURITY;

-- ── Criar role com acesso restrito (role da aplicação) ───────────────────────
CREATE ROLE app_user LOGIN PASSWORD 'strong_password';
GRANT CONNECT ON DATABASE erp_saas TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user;

-- ── Policy de isolamento por softhouse ───────────────────────────────────────
-- O Prisma seta a variável de sessão antes de cada transação:
-- SET LOCAL app.softhouse_id = '<uuid>';

CREATE POLICY softhouse_isolation ON empresas
  AS PERMISSIVE
  FOR ALL
  TO app_user
  USING (softhouse_id = current_setting('app.softhouse_id', true)::uuid);

CREATE POLICY empresa_isolation ON clientes
  AS PERMISSIVE
  FOR ALL
  TO app_user
  USING (
    softhouse_id = current_setting('app.softhouse_id', true)::uuid
    AND empresa_id = current_setting('app.empresa_id', true)::uuid
  );

-- ── Superuser bypassa RLS (para migrations, seeds) ──────────────────────────
-- O usuário de migrations/seeds deve ser superuser ou usar:
ALTER TABLE empresas FORCE ROW LEVEL SECURITY; -- Aplica até para owner
```

```typescript
// Setar contexto RLS no Prisma antes de cada query sensível
// shared/infrastructure/database/rls.service.ts
export class RlsService {
  constructor(private prisma: PrismaService) {}

  async withTenantContext<T>(
    softouseId: string,
    empresaId: string | null,
    fn: () => Promise<T>
  ): Promise<T> {
    return this.prisma.$transaction(async (tx) => {
      await tx.$executeRaw`SET LOCAL app.softhouse_id = ${softouseId}`;
      if (empresaId) {
        await tx.$executeRaw`SET LOCAL app.empresa_id = ${empresaId}`;
      }
      return fn();
    });
  }
}
```

### 3.1 Resolução do Tenant — Fluxo Completo

```
HTTP Request
    │
    ├─[RateLimitMiddleware]── Verifica IP + limite por tier de plano
    │
    ├─[JwtAuthGuard]────────── Valida token, extrai claims
    │                          Verifica blacklist no Redis (tokens revogados)
    │                          Valida softhouse_id (ativo? bloqueado?)
    │
    ├─[TenantMiddleware]─────── Inicializa AsyncLocalStorage<TenantContext>
    │                           Seta app.softhouse_id no connection
    │                           Valida empresa_id pertence à softhouse
    │
    ├─[PermissionsGuard]─────── Valida permissões do usuário para o endpoint
    │
    ├─[Route Handler]─────────── Lógica de negócio
    │   │
    │   └─[Prisma Middleware]── Injeta softhouse_id/empresa_id em toda query
    │       │
    │       └─[PostgreSQL RLS]─ Valida isolamento na camada de dados
    │
    ├─[AuditInterceptor]──────── Registra a ação em audit_logs
    │
    └─[TransformInterceptor]──── Formata resposta + remove campos sensíveis
```

---

## 4. Fluxo de Autenticação e JWT

### 4.1 Tokens

| Token         | TTL     | Armazenamento    | Refresh          |
|---------------|---------|------------------|------------------|
| Access Token  | 1 hora  | Memory (frontend)| Via Refresh Token|
| Refresh Token | 7 dias  | HttpOnly Cookie  | Rotação a cada uso|

**Rotação do Refresh Token:** a cada uso, o token antigo é invalidado no Redis e um novo é emitido. Se um token já usado é apresentado novamente, a sessão inteira é invalidada (indicativo de roubo de token).

### 4.2 Blacklist no Redis

```typescript
// Estrutura no Redis
SET token:blacklist:{jti} "revoked" EX 3600       // access token
SET refresh:blacklist:{refresh_jti} "used" EX 604800  // refresh token

// Verificação no JwtAuthGuard
const isBlacklisted = await redis.exists(`token:blacklist:${payload.jti}`);
if (isBlacklisted) throw new UnauthorizedException('Token revogado');
```

### 4.3 Fluxo de Login Multi-Nível

```
POST /auth/login
  Body: { email, password, softhouse_slug? }

1. Identifica o tipo de usuário (Master, Softhouse, Empresa)
2. Valida credenciais (bcrypt compare, custo 12)
3. Verifica status: ativo, não bloqueado, empresa não bloqueada
4. Verifica MFA (se habilitado): valida TOTP
5. Cria sessão no Redis com metadata (IP, User-Agent, geolocalização)
6. Gera Access Token + Refresh Token
7. Registra evento no audit_log
8. Retorna Access Token no body, Refresh Token em HttpOnly Cookie
```

### 4.4 Autenticação Multi-Softhouse

O mesmo email pode existir em softhouses diferentes (são tenants separados). A resolução ocorre por:

1. `softhouse_slug` no corpo da requisição OU subdomínio (`techsolutions.erp.com`)
2. O sistema filtra o usuário por `(email, softhouse_id)` — chave única composta

---

## 5. Modelagem do Banco de Dados — Prisma Schema Completo

```prisma
// prisma/schema.prisma
generator client {
  provider        = "prisma-client-js"
  previewFeatures = ["fullTextSearch", "postgresqlExtensions"]
}

datasource db {
  provider   = "postgresql"
  url        = env("DATABASE_URL")
  extensions = [uuidOssp(map: "uuid-ossp"), pgcrypto]
}

// ═══════════════════════════════════════════════════════════════════════════
// PLATAFORMA — sem tenant
// ═══════════════════════════════════════════════════════════════════════════

model SuperAdmin {
  id         String    @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  email      String    @unique
  name       String
  password   String
  is_active  Boolean   @default(true)
  created_at DateTime  @default(now()) @db.Timestamptz
  updated_at DateTime  @updatedAt @db.Timestamptz
  deleted_at DateTime? @db.Timestamptz

  @@map("super_admins")
}

model Plan {
  id              String    @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  name            String    @unique   // ex: "Starter", "Pro", "Enterprise"
  slug            String    @unique
  description     String?
  price_monthly   Decimal   @db.Decimal(10, 2)
  price_yearly    Decimal   @db.Decimal(10, 2)
  max_empresas    Int       @default(1)
  max_users       Int       @default(5)
  max_storage_gb  Int       @default(5)
  features        Json      @default("[]")   // lista de módulos habilitados
  is_active       Boolean   @default(true)
  created_at      DateTime  @default(now()) @db.Timestamptz
  updated_at      DateTime  @updatedAt @db.Timestamptz

  subscriptions Subscription[]

  @@map("plans")
}

// ═══════════════════════════════════════════════════════════════════════════
// SOFTHOUSE
// ═══════════════════════════════════════════════════════════════════════════

model Softhouse {
  id              String    @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  name            String
  slug            String    @unique          // subdomínio
  cnpj            String?   @unique
  email           String    @unique
  phone           String?
  logo_url        String?
  is_active       Boolean   @default(true)
  is_blocked      Boolean   @default(false)
  blocked_reason  String?
  blocked_at      DateTime? @db.Timestamptz
  settings        Json      @default("{}")  // configurações customizadas
  created_at      DateTime  @default(now()) @db.Timestamptz
  updated_at      DateTime  @updatedAt @db.Timestamptz
  deleted_at      DateTime? @db.Timestamptz

  users         SofthouseUser[]
  empresas      Empresa[]
  subscriptions Subscription[]

  @@map("softhouses")
}

model SofthouseUser {
  id           String    @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String    @db.Uuid
  email        String
  name         String
  password     String
  role         SofthouseRole @default(SUPORTE)
  is_active    Boolean   @default(true)
  mfa_secret   String?
  mfa_enabled  Boolean   @default(false)
  created_at   DateTime  @default(now()) @db.Timestamptz
  updated_at   DateTime  @updatedAt @db.Timestamptz
  deleted_at   DateTime? @db.Timestamptz

  softhouse Softhouse @relation(fields: [softhouse_id], references: [id])

  @@unique([softhouse_id, email])
  @@index([softhouse_id])
  @@map("softhouse_users")
}

model Subscription {
  id              String             @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id    String             @db.Uuid
  plan_id         String             @db.Uuid
  status          SubscriptionStatus @default(TRIAL)
  trial_ends_at   DateTime?          @db.Timestamptz
  current_period_start DateTime      @db.Timestamptz
  current_period_end   DateTime      @db.Timestamptz
  canceled_at     DateTime?          @db.Timestamptz
  created_at      DateTime           @default(now()) @db.Timestamptz
  updated_at      DateTime           @updatedAt @db.Timestamptz

  softhouse Softhouse @relation(fields: [softhouse_id], references: [id])
  plan      Plan      @relation(fields: [plan_id], references: [id])

  @@index([softhouse_id])
  @@map("subscriptions")
}

// ═══════════════════════════════════════════════════════════════════════════
// EMPRESA (CLIENTE DA SOFTHOUSE)
// ═══════════════════════════════════════════════════════════════════════════

model Empresa {
  id              String    @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id    String    @db.Uuid
  name            String
  trade_name      String?
  cnpj            String
  ie              String?   // Inscrição Estadual
  im              String?   // Inscrição Municipal
  email           String?
  phone           String?
  logo_url        String?
  is_active       Boolean   @default(true)
  is_blocked      Boolean   @default(false)
  blocked_reason  String?
  blocked_at      DateTime? @db.Timestamptz
  regime_tributario  RegimeTributario @default(SIMPLES_NACIONAL)
  address         Json?
  settings        Json      @default("{}")
  created_at      DateTime  @default(now()) @db.Timestamptz
  updated_at      DateTime  @updatedAt @db.Timestamptz
  deleted_at      DateTime? @db.Timestamptz

  softhouse Softhouse       @relation(fields: [softhouse_id], references: [id])
  users     EmpresaUser[]
  modulos   EmpresaModulo[]

  // Dados do ERP
  clientes            Cliente[]
  produtos            Produto[]
  pedidos             Pedido[]
  contas_pagar        ContaPagar[]
  contas_receber      ContaReceber[]
  lancamentos         Lancamento[]
  nfe                 Nfe[]
  crm_leads           CrmLead[]

  @@unique([softhouse_id, cnpj])
  @@index([softhouse_id])
  @@map("empresas")
}

model EmpresaModulo {
  id           String   @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String   @db.Uuid
  empresa_id   String   @db.Uuid
  modulo       Modulo
  is_enabled   Boolean  @default(true)
  config       Json     @default("{}")
  created_at   DateTime @default(now()) @db.Timestamptz
  updated_at   DateTime @updatedAt @db.Timestamptz

  empresa Empresa @relation(fields: [empresa_id], references: [id])

  @@unique([empresa_id, modulo])
  @@index([softhouse_id, empresa_id])
  @@map("empresa_modulos")
}

// ═══════════════════════════════════════════════════════════════════════════
// USUÁRIOS E PERMISSÕES
// ═══════════════════════════════════════════════════════════════════════════

model EmpresaUser {
  id           String    @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String    @db.Uuid
  empresa_id   String    @db.Uuid
  email        String
  name         String
  password     String
  is_active    Boolean   @default(true)
  mfa_secret   String?
  mfa_enabled  Boolean   @default(false)
  last_login_at DateTime? @db.Timestamptz
  created_at   DateTime  @default(now()) @db.Timestamptz
  updated_at   DateTime  @updatedAt @db.Timestamptz
  deleted_at   DateTime? @db.Timestamptz

  empresa  Empresa      @relation(fields: [empresa_id], references: [id])
  roles    UserRole[]
  sessions Session[]

  @@unique([softhouse_id, empresa_id, email])
  @@index([softhouse_id, empresa_id])
  @@map("empresa_users")
}

model Role {
  id           String    @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String?   @db.Uuid   // null = role global do sistema
  empresa_id   String?   @db.Uuid   // null = role de softhouse
  name         String                // ex: "Financeiro Senior"
  slug         String                // ex: "financeiro_senior"
  description  String?
  is_system    Boolean   @default(false)  // true = não pode ser alterado
  created_at   DateTime  @default(now()) @db.Timestamptz
  updated_at   DateTime  @updatedAt @db.Timestamptz

  permissions RolePermission[]
  user_roles  UserRole[]

  @@unique([softhouse_id, empresa_id, slug])
  @@map("roles")
}

model Permission {
  id          String   @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  resource    String   // ex: "financeiro", "estoque", "nfe"
  action      String   // ex: "visualizar", "criar", "editar", "excluir", "aprovar"
  scope       String?  // ex: "empresa", "softhouse" (opcional)
  description String?
  created_at  DateTime @default(now()) @db.Timestamptz

  role_permissions RolePermission[]

  @@unique([resource, action, scope])
  @@map("permissions")
}

model RolePermission {
  role_id       String @db.Uuid
  permission_id String @db.Uuid

  role       Role       @relation(fields: [role_id], references: [id], onDelete: Cascade)
  permission Permission @relation(fields: [permission_id], references: [id], onDelete: Cascade)

  @@id([role_id, permission_id])
  @@map("role_permissions")
}

model UserRole {
  user_id    String   @db.Uuid
  role_id    String   @db.Uuid
  granted_by String   @db.Uuid
  granted_at DateTime @default(now()) @db.Timestamptz

  user EmpresaUser @relation(fields: [user_id], references: [id], onDelete: Cascade)
  role Role        @relation(fields: [role_id], references: [id], onDelete: Cascade)

  @@id([user_id, role_id])
  @@map("user_roles")
}

model Session {
  id           String    @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  user_id      String    @db.Uuid
  softhouse_id String    @db.Uuid
  empresa_id   String?   @db.Uuid
  jti          String    @unique   // JWT ID do access token
  refresh_jti  String    @unique   // JWT ID do refresh token
  ip_address   String?
  user_agent   String?
  is_active    Boolean   @default(true)
  expires_at   DateTime  @db.Timestamptz
  created_at   DateTime  @default(now()) @db.Timestamptz

  user EmpresaUser @relation(fields: [user_id], references: [id])

  @@index([user_id])
  @@index([jti])
  @@map("sessions")
}

// ═══════════════════════════════════════════════════════════════════════════
// CLIENTES / CONTATOS
// ═══════════════════════════════════════════════════════════════════════════

model Cliente {
  id               String      @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id     String      @db.Uuid
  empresa_id       String      @db.Uuid
  tipo             TipoPessoa  @default(JURIDICA)
  name             String
  cpf_cnpj         String
  ie               String?
  email            String?
  phone            String?
  whatsapp         String?
  address          Json?
  is_active        Boolean     @default(true)
  is_blocked       Boolean     @default(false)
  credit_limit     Decimal?    @db.Decimal(15, 2)
  observations     String?
  tags             String[]
  created_at       DateTime    @default(now()) @db.Timestamptz
  updated_at       DateTime    @updatedAt @db.Timestamptz
  deleted_at       DateTime?   @db.Timestamptz

  empresa   Empresa    @relation(fields: [empresa_id], references: [id])
  contatos  Contato[]
  pedidos   Pedido[]
  historico ClienteHistorico[]

  @@unique([softhouse_id, empresa_id, cpf_cnpj])
  @@index([softhouse_id, empresa_id])
  @@index([softhouse_id, empresa_id, name])
  @@map("clientes")
}

model Contato {
  id           String   @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String   @db.Uuid
  empresa_id   String   @db.Uuid
  cliente_id   String   @db.Uuid
  name         String
  cargo        String?
  email        String?
  phone        String?
  is_primary   Boolean  @default(false)
  created_at   DateTime @default(now()) @db.Timestamptz
  updated_at   DateTime @updatedAt @db.Timestamptz
  deleted_at   DateTime? @db.Timestamptz

  cliente Cliente @relation(fields: [cliente_id], references: [id])

  @@index([softhouse_id, empresa_id, cliente_id])
  @@map("contatos")
}

model ClienteHistorico {
  id           String   @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String   @db.Uuid
  empresa_id   String   @db.Uuid
  cliente_id   String   @db.Uuid
  tipo         String   // ex: "BLOQUEIO", "OBSERVACAO", "PEDIDO", "PAGAMENTO"
  description  String
  user_id      String   @db.Uuid
  metadata     Json?
  created_at   DateTime @default(now()) @db.Timestamptz

  cliente Cliente @relation(fields: [cliente_id], references: [id])

  @@index([softhouse_id, empresa_id, cliente_id])
  @@map("cliente_historicos")
}

// ═══════════════════════════════════════════════════════════════════════════
// FINANCEIRO
// ═══════════════════════════════════════════════════════════════════════════

model CategoriaFinanceira {
  id           String   @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String   @db.Uuid
  empresa_id   String   @db.Uuid
  name         String
  tipo         TipoCategoria
  parent_id    String?  @db.Uuid
  color        String?
  is_active    Boolean  @default(true)
  created_at   DateTime @default(now()) @db.Timestamptz
  updated_at   DateTime @updatedAt @db.Timestamptz
  deleted_at   DateTime? @db.Timestamptz

  parent   CategoriaFinanceira?  @relation("CategoriaHierarquia", fields: [parent_id], references: [id])
  children CategoriaFinanceira[] @relation("CategoriaHierarquia")

  @@unique([softhouse_id, empresa_id, name, tipo])
  @@index([softhouse_id, empresa_id])
  @@map("categorias_financeiras")
}

model CentroCusto {
  id           String   @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String   @db.Uuid
  empresa_id   String   @db.Uuid
  name         String
  code         String
  is_active    Boolean  @default(true)
  created_at   DateTime @default(now()) @db.Timestamptz
  updated_at   DateTime @updatedAt @db.Timestamptz
  deleted_at   DateTime? @db.Timestamptz

  @@unique([softhouse_id, empresa_id, code])
  @@index([softhouse_id, empresa_id])
  @@map("centros_custo")
}

model ContaPagar {
  id                String           @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id      String           @db.Uuid
  empresa_id        String           @db.Uuid
  fornecedor_id     String?          @db.Uuid
  categoria_id      String?          @db.Uuid
  centro_custo_id   String?          @db.Uuid
  description       String
  valor             Decimal          @db.Decimal(15, 2)
  valor_pago        Decimal          @default(0) @db.Decimal(15, 2)
  data_emissao      DateTime         @db.Date
  data_vencimento   DateTime         @db.Date
  data_pagamento    DateTime?        @db.Date
  status            StatusFinanceiro @default(PENDENTE)
  numero_documento  String?
  observacoes       String?
  attachments       String[]
  recorrente        Boolean          @default(false)
  recorrencia_config Json?
  created_at        DateTime         @default(now()) @db.Timestamptz
  updated_at        DateTime         @updatedAt @db.Timestamptz
  deleted_at        DateTime?        @db.Timestamptz

  empresa    Empresa              @relation(fields: [empresa_id], references: [id])
  lancamentos Lancamento[]

  @@index([softhouse_id, empresa_id, status, data_vencimento])
  @@index([softhouse_id, empresa_id, data_vencimento])
  @@map("contas_pagar")
}

model ContaReceber {
  id                String           @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id      String           @db.Uuid
  empresa_id        String           @db.Uuid
  cliente_id        String?          @db.Uuid
  pedido_id         String?          @db.Uuid
  categoria_id      String?          @db.Uuid
  centro_custo_id   String?          @db.Uuid
  description       String
  valor             Decimal          @db.Decimal(15, 2)
  valor_recebido    Decimal          @default(0) @db.Decimal(15, 2)
  data_emissao      DateTime         @db.Date
  data_vencimento   DateTime         @db.Date
  data_recebimento  DateTime?        @db.Date
  status            StatusFinanceiro @default(PENDENTE)
  numero_documento  String?
  observacoes       String?
  created_at        DateTime         @default(now()) @db.Timestamptz
  updated_at        DateTime         @updatedAt @db.Timestamptz
  deleted_at        DateTime?        @db.Timestamptz

  empresa     Empresa      @relation(fields: [empresa_id], references: [id])
  lancamentos Lancamento[]

  @@index([softhouse_id, empresa_id, status, data_vencimento])
  @@index([softhouse_id, empresa_id, cliente_id])
  @@map("contas_receber")
}

model Lancamento {
  id               String         @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id     String         @db.Uuid
  empresa_id       String         @db.Uuid
  conta_pagar_id   String?        @db.Uuid
  conta_receber_id String?        @db.Uuid
  categoria_id     String?        @db.Uuid
  centro_custo_id  String?        @db.Uuid
  tipo             TipoLancamento
  valor            Decimal        @db.Decimal(15, 2)
  data             DateTime       @db.Date
  description      String
  forma_pagamento  FormaPagamento
  conta_bancaria   String?
  conciliado       Boolean        @default(false)
  user_id          String         @db.Uuid
  created_at       DateTime       @default(now()) @db.Timestamptz
  updated_at       DateTime       @updatedAt @db.Timestamptz

  empresa       Empresa       @relation(fields: [empresa_id], references: [id])
  conta_pagar   ContaPagar?   @relation(fields: [conta_pagar_id], references: [id])
  conta_receber ContaReceber? @relation(fields: [conta_receber_id], references: [id])

  @@index([softhouse_id, empresa_id, data])
  @@index([softhouse_id, empresa_id, tipo, data])
  @@map("lancamentos")
}

// ═══════════════════════════════════════════════════════════════════════════
// ESTOQUE / PRODUTOS
// ═══════════════════════════════════════════════════════════════════════════

model CategoriaProduto {
  id           String   @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String   @db.Uuid
  empresa_id   String   @db.Uuid
  name         String
  parent_id    String?  @db.Uuid
  created_at   DateTime @default(now()) @db.Timestamptz
  updated_at   DateTime @updatedAt @db.Timestamptz
  deleted_at   DateTime? @db.Timestamptz

  parent   CategoriaProduto?  @relation("CategoriaHierarquia", fields: [parent_id], references: [id])
  children CategoriaProduto[] @relation("CategoriaHierarquia")
  produtos Produto[]

  @@index([softhouse_id, empresa_id])
  @@map("categorias_produtos")
}

model Produto {
  id              String    @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id    String    @db.Uuid
  empresa_id      String    @db.Uuid
  categoria_id    String?   @db.Uuid
  name            String
  description     String?
  sku             String
  barcode         String?
  unit            String    @default("UN")
  price_cost      Decimal   @default(0) @db.Decimal(15, 4)
  price_sale      Decimal   @db.Decimal(15, 2)
  price_min       Decimal?  @db.Decimal(15, 2)
  stock_atual     Decimal   @default(0) @db.Decimal(15, 4)
  stock_minimo    Decimal   @default(0) @db.Decimal(15, 4)
  stock_maximo    Decimal?  @db.Decimal(15, 4)
  controla_estoque Boolean  @default(true)
  is_active       Boolean   @default(true)
  ncm             String?
  cst             String?
  cfop            String?
  images          String[]
  attributes      Json      @default("{}")
  created_at      DateTime  @default(now()) @db.Timestamptz
  updated_at      DateTime  @updatedAt @db.Timestamptz
  deleted_at      DateTime? @db.Timestamptz

  empresa   Empresa          @relation(fields: [empresa_id], references: [id])
  categoria CategoriaProduto? @relation(fields: [categoria_id], references: [id])
  movimentos MovimentoEstoque[]

  @@unique([softhouse_id, empresa_id, sku])
  @@index([softhouse_id, empresa_id])
  @@index([softhouse_id, empresa_id, name])
  @@index([softhouse_id, empresa_id, barcode])
  @@map("produtos")
}

model MovimentoEstoque {
  id              String          @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id    String          @db.Uuid
  empresa_id      String          @db.Uuid
  produto_id      String          @db.Uuid
  tipo            TipoMovimento
  quantidade      Decimal         @db.Decimal(15, 4)
  saldo_anterior  Decimal         @db.Decimal(15, 4)
  saldo_posterior Decimal         @db.Decimal(15, 4)
  custo_unitario  Decimal?        @db.Decimal(15, 4)
  referencia_tipo String?         // "PEDIDO", "NF_ENTRADA", "INVENTARIO"
  referencia_id   String?         @db.Uuid
  user_id         String          @db.Uuid
  observations    String?
  created_at      DateTime        @default(now()) @db.Timestamptz

  produto Produto @relation(fields: [produto_id], references: [id])

  @@index([softhouse_id, empresa_id, produto_id, created_at])
  @@index([softhouse_id, empresa_id, tipo, created_at])
  @@map("movimentos_estoque")
}

// ═══════════════════════════════════════════════════════════════════════════
// VENDAS / PEDIDOS
// ═══════════════════════════════════════════════════════════════════════════

model Pedido {
  id              String       @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id    String       @db.Uuid
  empresa_id      String       @db.Uuid
  cliente_id      String?      @db.Uuid
  numero          Int
  tipo            TipoPedido   @default(VENDA)
  status          StatusPedido @default(RASCUNHO)
  data_emissao    DateTime     @db.Date
  data_entrega    DateTime?    @db.Date
  subtotal        Decimal      @db.Decimal(15, 2)
  desconto        Decimal      @default(0) @db.Decimal(15, 2)
  frete           Decimal      @default(0) @db.Decimal(15, 2)
  total           Decimal      @db.Decimal(15, 2)
  observacoes     String?
  vendedor_id     String?      @db.Uuid
  forma_pagamento FormaPagamento?
  nfe_id          String?      @db.Uuid
  created_at      DateTime     @default(now()) @db.Timestamptz
  updated_at      DateTime     @updatedAt @db.Timestamptz
  deleted_at      DateTime?    @db.Timestamptz

  empresa  Empresa      @relation(fields: [empresa_id], references: [id])
  cliente  Cliente?     @relation(fields: [cliente_id], references: [id])
  itens    ItemPedido[]

  @@unique([softhouse_id, empresa_id, numero])
  @@index([softhouse_id, empresa_id, status])
  @@index([softhouse_id, empresa_id, data_emissao])
  @@index([softhouse_id, empresa_id, cliente_id])
  @@map("pedidos")
}

model ItemPedido {
  id            String  @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id  String  @db.Uuid
  empresa_id    String  @db.Uuid
  pedido_id     String  @db.Uuid
  produto_id    String  @db.Uuid
  quantidade    Decimal @db.Decimal(15, 4)
  preco_custo   Decimal @db.Decimal(15, 4)
  preco_venda   Decimal @db.Decimal(15, 2)
  desconto      Decimal @default(0) @db.Decimal(15, 2)
  total         Decimal @db.Decimal(15, 2)
  created_at    DateTime @default(now()) @db.Timestamptz

  pedido  Pedido  @relation(fields: [pedido_id], references: [id], onDelete: Cascade)

  @@index([softhouse_id, empresa_id, pedido_id])
  @@map("itens_pedido")
}

// ═══════════════════════════════════════════════════════════════════════════
// FISCAL
// ═══════════════════════════════════════════════════════════════════════════

model Nfe {
  id              String    @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id    String    @db.Uuid
  empresa_id      String    @db.Uuid
  pedido_id       String?   @db.Uuid
  tipo            TipoNfe
  modelo          String    // "55" NFe, "65" NFCe
  serie           String    @default("1")
  numero          Int
  chave_acesso    String?   @unique
  status          StatusNfe @default(RASCUNHO)
  data_emissao    DateTime  @db.Timestamptz
  valor_total     Decimal   @db.Decimal(15, 2)
  xml_enviado     String?   @db.Text
  xml_retorno     String?   @db.Text
  pdf_url         String?
  protocolo       String?
  ambiente        AmbienteNfe @default(HOMOLOGACAO)
  created_at      DateTime  @default(now()) @db.Timestamptz
  updated_at      DateTime  @updatedAt @db.Timestamptz

  empresa Empresa @relation(fields: [empresa_id], references: [id])

  @@unique([softhouse_id, empresa_id, modelo, serie, numero])
  @@index([softhouse_id, empresa_id, status])
  @@index([softhouse_id, empresa_id, data_emissao])
  @@map("nfe")
}

model CertificadoDigital {
  id              String    @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id    String    @db.Uuid
  empresa_id      String    @db.Uuid
  tipo            String    // "A1", "A3"
  nome_titular    String
  cnpj            String
  validade        DateTime  @db.Date
  pfx_encrypted   String    @db.Text   // certificado criptografado
  is_active       Boolean   @default(true)
  created_at      DateTime  @default(now()) @db.Timestamptz
  updated_at      DateTime  @updatedAt @db.Timestamptz

  @@index([softhouse_id, empresa_id])
  @@map("certificados_digitais")
}

// ═══════════════════════════════════════════════════════════════════════════
// CRM
// ═══════════════════════════════════════════════════════════════════════════

model CrmPipeline {
  id           String   @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String   @db.Uuid
  empresa_id   String   @db.Uuid
  name         String
  is_active    Boolean  @default(true)
  created_at   DateTime @default(now()) @db.Timestamptz

  etapas CrmEtapa[]

  @@index([softhouse_id, empresa_id])
  @@map("crm_pipelines")
}

model CrmEtapa {
  id           String   @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String   @db.Uuid
  empresa_id   String   @db.Uuid
  pipeline_id  String   @db.Uuid
  name         String
  ordem        Int
  color        String?
  is_final     Boolean  @default(false)
  is_won       Boolean  @default(false)
  created_at   DateTime @default(now()) @db.Timestamptz

  pipeline CrmPipeline @relation(fields: [pipeline_id], references: [id])
  leads    CrmLead[]

  @@index([softhouse_id, empresa_id, pipeline_id])
  @@map("crm_etapas")
}

model CrmLead {
  id              String      @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id    String      @db.Uuid
  empresa_id      String      @db.Uuid
  etapa_id        String      @db.Uuid
  cliente_id      String?     @db.Uuid
  responsavel_id  String?     @db.Uuid
  title           String
  valor_estimado  Decimal?    @db.Decimal(15, 2)
  probabilidade   Int?        // 0-100
  data_fechamento DateTime?   @db.Date
  status          StatusLead  @default(ABERTO)
  origem          String?
  observations    String?
  tags            String[]
  created_at      DateTime    @default(now()) @db.Timestamptz
  updated_at      DateTime    @updatedAt @db.Timestamptz
  deleted_at      DateTime?   @db.Timestamptz

  empresa    Empresa    @relation(fields: [empresa_id], references: [id])
  etapa      CrmEtapa   @relation(fields: [etapa_id], references: [id])
  atividades CrmAtividade[]

  @@index([softhouse_id, empresa_id, status])
  @@index([softhouse_id, empresa_id, etapa_id])
  @@map("crm_leads")
}

model CrmAtividade {
  id           String         @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String         @db.Uuid
  empresa_id   String         @db.Uuid
  lead_id      String         @db.Uuid
  user_id      String         @db.Uuid
  tipo         TipoAtividade
  title        String
  description  String?
  data_inicio  DateTime       @db.Timestamptz
  data_fim     DateTime?      @db.Timestamptz
  concluida    Boolean        @default(false)
  created_at   DateTime       @default(now()) @db.Timestamptz
  updated_at   DateTime       @updatedAt @db.Timestamptz

  lead CrmLead @relation(fields: [lead_id], references: [id])

  @@index([softhouse_id, empresa_id, lead_id])
  @@index([softhouse_id, empresa_id, user_id, data_inicio])
  @@map("crm_atividades")
}

// ═══════════════════════════════════════════════════════════════════════════
// AUDITORIA
// ═══════════════════════════════════════════════════════════════════════════

model AuditLog {
  id           String   @id @default(dbgenerated("gen_random_uuid()")) @db.Uuid
  softhouse_id String?  @db.Uuid   // null para ações de Master
  empresa_id   String?  @db.Uuid
  user_id      String?  @db.Uuid
  user_type    String   // "MASTER", "SOFTHOUSE", "EMPRESA"
  action       String   // ex: "cliente.criado", "pedido.aprovado"
  resource     String   // ex: "clientes"
  resource_id  String?  @db.Uuid
  ip_address   String?
  user_agent   String?
  before       Json?    // estado anterior (para updates)
  after        Json?    // estado novo
  metadata     Json?
  created_at   DateTime @default(now()) @db.Timestamptz

  @@index([softhouse_id, empresa_id, created_at])
  @@index([softhouse_id, empresa_id, user_id, created_at])
  @@index([softhouse_id, empresa_id, resource, resource_id])
  @@map("audit_logs")
}

// ═══════════════════════════════════════════════════════════════════════════
// ENUMS
// ═══════════════════════════════════════════════════════════════════════════

enum SofthouseRole {
  ADMIN
  FINANCEIRO
  SUPORTE
  VIEWER
}

enum SubscriptionStatus {
  TRIAL
  ACTIVE
  PAST_DUE
  CANCELED
  SUSPENDED
}

enum Modulo {
  FINANCEIRO
  ESTOQUE
  VENDAS
  FISCAL
  CRM
  PDV
  RELATORIOS
}

enum RegimeTributario {
  SIMPLES_NACIONAL
  LUCRO_PRESUMIDO
  LUCRO_REAL
  MEI
}

enum TipoPessoa {
  FISICA
  JURIDICA
}

enum TipoCategoria {
  RECEITA
  DESPESA
}

enum StatusFinanceiro {
  PENDENTE
  PAGO
  PARCIAL
  VENCIDO
  CANCELADO
}

enum TipoLancamento {
  RECEITA
  DESPESA
  TRANSFERENCIA
}

enum FormaPagamento {
  DINHEIRO
  PIX
  BOLETO
  CARTAO_CREDITO
  CARTAO_DEBITO
  TRANSFERENCIA
  CHEQUE
}

enum TipoMovimento {
  ENTRADA
  SAIDA
  AJUSTE_POSITIVO
  AJUSTE_NEGATIVO
  INVENTARIO
  DEVOLUCAO
}

enum TipoPedido {
  VENDA
  ORCAMENTO
  DEVOLUCAO
  BONIFICACAO
}

enum StatusPedido {
  RASCUNHO
  AGUARDANDO_APROVACAO
  APROVADO
  EM_SEPARACAO
  FATURADO
  ENTREGUE
  CANCELADO
}

enum TipoNfe {
  SAIDA
  ENTRADA
  DEVOLUCAO
}

enum StatusNfe {
  RASCUNHO
  ENVIADA
  AUTORIZADA
  CANCELADA
  DENEGADA
  REJEITADA
}

enum AmbienteNfe {
  PRODUCAO
  HOMOLOGACAO
}

enum StatusLead {
  ABERTO
  GANHO
  PERDIDO
  ARQUIVADO
}

enum TipoAtividade {
  LIGACAO
  EMAIL
  REUNIAO
  VISITA
  TAREFA
  NOTA
}
```

---

## 6. RBAC Avançado — Permissões e Roles

### 6.1 Formato de Permissão

```
{recurso}:{ação}[:{escopo}]

Exemplos:
  clientes:visualizar
  clientes:criar
  clientes:editar
  clientes:excluir
  clientes:bloquear
  financeiro:visualizar
  financeiro:criar
  financeiro:editar
  financeiro:excluir
  financeiro:aprovar
  pedidos:visualizar
  pedidos:criar
  pedidos:aprovar
  nfe:emitir
  nfe:cancelar
  relatorios:exportar:pdf
  relatorios:exportar:excel
  usuarios:criar:empresa
  usuarios:criar:softhouse
  estoque:ajustar
  estoque:inventario
```

### 6.2 Matriz de Permissões por Role

| Permissão                  | SUPER_ADMIN | SOFT_ADMIN | SOFT_SUPORTE | EMP_ADMIN | EMP_FINANCEIRO | EMP_VENDAS | EMP_ESTOQUE | EMP_VIEWER |
|----------------------------|:-----------:|:----------:|:------------:|:---------:|:--------------:|:----------:|:-----------:|:----------:|
| clientes:visualizar        | ✓           | ✓          | ✓            | ✓         | ✓              | ✓          | ✓           | ✓          |
| clientes:criar             | ✓           | ✓          | ✓            | ✓         | ✗              | ✓          | ✗           | ✗          |
| clientes:editar            | ✓           | ✓          | ✓            | ✓         | ✗              | ✓          | ✗           | ✗          |
| clientes:bloquear          | ✓           | ✓          | ✗            | ✓         | ✗              | ✗          | ✗           | ✗          |
| financeiro:visualizar      | ✓           | ✓          | ✗            | ✓         | ✓              | ✗          | ✗           | ✗          |
| financeiro:criar           | ✓           | ✓          | ✗            | ✓         | ✓              | ✗          | ✗           | ✗          |
| financeiro:aprovar         | ✓           | ✓          | ✗            | ✓         | ✗              | ✗          | ✗           | ✗          |
| pedidos:criar              | ✓           | ✓          | ✗            | ✓         | ✗              | ✓          | ✗           | ✗          |
| pedidos:aprovar            | ✓           | ✓          | ✗            | ✓         | ✓              | ✗          | ✗           | ✗          |
| estoque:visualizar         | ✓           | ✓          | ✓            | ✓         | ✗              | ✓          | ✓           | ✓          |
| estoque:ajustar            | ✓           | ✓          | ✗            | ✓         | ✗              | ✗          | ✓           | ✗          |
| nfe:emitir                 | ✓           | ✓          | ✗            | ✓         | ✓              | ✗          | ✗           | ✗          |
| usuarios:criar:empresa     | ✓           | ✓          | ✗            | ✓         | ✗              | ✗          | ✗           | ✗          |
| usuarios:criar:softhouse   | ✓           | ✓          | ✗            | ✗         | ✗              | ✗          | ✗           | ✗          |
| relatorios:exportar:pdf    | ✓           | ✓          | ✓            | ✓         | ✓              | ✓          | ✓           | ✗          |

### 6.3 Hierarquia de Roles

```
SUPER_ADMIN
  └── [acesso irrestrito à plataforma]

SOFTHOUSE_ADMIN
  ├── Gerencia empresas/clientes da softhouse
  ├── Cria/remove usuários de softhouse e de empresas
  ├── Configura módulos por empresa
  └── Visualiza relatórios da softhouse

SOFTHOUSE_FINANCEIRO
  ├── Visualiza cobrança e assinatura
  └── Emite relatórios financeiros da softhouse

SOFTHOUSE_SUPORTE
  ├── Visualiza empresas e usuários
  └── Não pode alterar dados financeiros

EMPRESA_ADMIN
  ├── Controle total do ERP da empresa
  ├── Cria/remove usuários da empresa
  └── Configura o sistema

EMPRESA_FINANCEIRO
  ├── Módulos: Financeiro, Relatórios
  └── Pode aprovar lançamentos

EMPRESA_VENDAS
  ├── Módulos: Clientes, Pedidos, Orçamentos, CRM
  └── Não acessa módulo financeiro

EMPRESA_ESTOQUE
  ├── Módulos: Produtos, Estoque
  └── Visualiza pedidos (somente leitura)

EMPRESA_VIEWER
  └── Somente leitura em todos os módulos habilitados
```

---

## 7. Middlewares, Guards e Decorators

### 7.1 TenantContext com AsyncLocalStorage

```typescript
// shared/infrastructure/tenant/tenant-context.service.ts
import { AsyncLocalStorage } from 'async_hooks';
import { Injectable } from '@nestjs/common';

export interface TenantContext {
  softouseId: string;
  empresaId: string | null;
  userId: string;
  userType: 'MASTER' | 'SOFTHOUSE' | 'EMPRESA';
  role: string;
  permissions: string[];
  sessionId: string;
  ipAddress?: string;
}

export const tenantStorage = new AsyncLocalStorage<TenantContext>();

@Injectable()
export class TenantContextService {
  get(): TenantContext {
    const ctx = tenantStorage.getStore();
    if (!ctx) throw new Error('TenantContext não disponível fora de uma requisição HTTP');
    return ctx;
  }

  get softouseId(): string { return this.get().softouseId; }
  get empresaId(): string | null { return this.get().empresaId; }
  get userId(): string { return this.get().userId; }
  get permissions(): string[] { return this.get().permissions; }

  hasPermission(permission: string): boolean {
    return this.get().permissions.includes(permission);
  }

  requirePermission(permission: string): void {
    if (!this.hasPermission(permission)) {
      throw new ForbiddenException(`Permissão necessária: ${permission}`);
    }
  }
}
```

### 7.2 Tenant Middleware

```typescript
// shared/middleware/tenant.middleware.ts
@Injectable()
export class TenantMiddleware implements NestMiddleware {
  constructor(
    private jwtService: JwtService,
    private tenantValidator: TenantValidatorService,
    private redis: RedisService,
  ) {}

  async use(req: Request, res: Response, next: NextFunction) {
    const token = this.extractToken(req);
    if (!token) return next();

    const payload: JwtPayload = this.jwtService.verify(token);

    // Verificar blacklist
    const blacklisted = await this.redis.exists(`token:blacklist:${payload.jti}`);
    if (blacklisted) throw new UnauthorizedException('Sessão inválida');

    // Validar softhouse ativo
    await this.tenantValidator.validateSofthouse(payload.tenant.softhouse_id);

    // Inicializar contexto na thread local
    const context: TenantContext = {
      softouseId: payload.tenant.softhouse_id,
      empresaId: payload.tenant.empresa_id,
      userId: payload.sub,
      userType: this.resolveUserType(payload.role),
      role: payload.role,
      permissions: payload.permissions,
      sessionId: payload.jti,
      ipAddress: req.ip,
    };

    tenantStorage.run(context, () => next());
  }

  private extractToken(req: Request): string | null {
    const auth = req.headers.authorization;
    return auth?.startsWith('Bearer ') ? auth.slice(7) : null;
  }
}
```

### 7.3 PermissionsGuard e Decorator

```typescript
// shared/guards/permissions.guard.ts
export const PERMISSIONS_KEY = 'permissions';

export const RequirePermissions = (...permissions: string[]) =>
  SetMetadata(PERMISSIONS_KEY, permissions);

@Injectable()
export class PermissionsGuard implements CanActivate {
  constructor(private reflector: Reflector) {}

  canActivate(context: ExecutionContext): boolean {
    const required = this.reflector.getAllAndOverride<string[]>(PERMISSIONS_KEY, [
      context.getHandler(),
      context.getClass(),
    ]);

    if (!required?.length) return true;

    const ctx = tenantStorage.getStore();
    if (!ctx) throw new UnauthorizedException();

    const hasAll = required.every(perm => ctx.permissions.includes(perm));
    if (!hasAll) throw new ForbiddenException(
      `Permissões insuficientes. Necessário: ${required.join(', ')}`
    );

    return true;
  }
}

// Uso no controller:
@Get('contas')
@RequirePermissions('financeiro:visualizar')
async listarContas(@Query() query: ListarContasDto) { ... }

@Post('contas')
@RequirePermissions('financeiro:criar')
async criarConta(@Body() body: CriarContaDto) { ... }

@Patch('contas/:id/aprovar')
@RequirePermissions('financeiro:aprovar')
async aprovarConta(@Param('id') id: string) { ... }
```

### 7.4 AuditInterceptor

```typescript
// shared/interceptors/audit.interceptor.ts
@Injectable()
export class AuditInterceptor implements NestInterceptor {
  constructor(private auditService: AuditService) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const req = context.switchToHttp().getRequest();
    const ctx = tenantStorage.getStore();
    const startTime = Date.now();

    return next.handle().pipe(
      tap(async (responseData) => {
        if (!ctx || req.method === 'GET') return; // não audita leituras

        await this.auditService.log({
          softhouse_id: ctx.softouseId,
          empresa_id: ctx.empresaId,
          user_id: ctx.userId,
          user_type: ctx.userType,
          action: `${req.method.toLowerCase()}.${req.route.path}`,
          resource: this.extractResource(req.route.path),
          resource_id: req.params.id,
          ip_address: ctx.ipAddress,
          user_agent: req.headers['user-agent'],
          after: responseData,
          metadata: { duration_ms: Date.now() - startTime },
        });
      }),
    );
  }
}
```

---

## 8. Estratégia de Índices e Performance

### 8.1 Padrão de Índices para Tabelas de Tenant

```sql
-- Todo registro tem softhouse_id. Índice parcial filtra soft-deleted.
CREATE INDEX idx_{tabela}_softhouse
  ON {tabela}(softhouse_id)
  WHERE deleted_at IS NULL;

-- Tabelas de empresa: índice composto (tenant completo)
CREATE INDEX idx_{tabela}_tenant
  ON {tabela}(softhouse_id, empresa_id)
  WHERE deleted_at IS NULL;

-- Índice para queries de negócio específicas (os mais importantes)
-- Financeiro: listagem com filtro de status e data
CREATE INDEX idx_contas_pagar_listagem
  ON contas_pagar(empresa_id, status, data_vencimento)
  WHERE deleted_at IS NULL;

-- Produtos: busca textual
CREATE INDEX idx_produtos_busca
  ON produtos USING GIN (to_tsvector('portuguese', name || ' ' || COALESCE(description, '')))
  WHERE deleted_at IS NULL;

-- Auditoria: range scan por data
CREATE INDEX idx_audit_logs_range
  ON audit_logs(softhouse_id, empresa_id, created_at DESC);
```

### 8.2 Query Budget por Endpoint

| Endpoint                          | Máx. Queries | Cache Redis TTL |
|-----------------------------------|:------------:|:---------------:|
| GET /dashboard                    | 1 (view)     | 5 min           |
| GET /clientes (listagem)          | 1            | 30s             |
| GET /clientes/:id                 | 2            | 1 min           |
| GET /financeiro/fluxo-de-caixa    | 1 (view)     | 2 min           |
| GET /estoque/posicao              | 1 (view)     | 1 min           |
| POST /pedidos                     | 3 (tx)       | —               |
| POST /nfe/emitir                  | 5 (tx + queue)| —              |

### 8.3 Paginação Cursor-based

Para tabelas com alto volume (pedidos, lançamentos), usar cursor pagination em vez de OFFSET:

```typescript
// Mais eficiente que LIMIT/OFFSET em tabelas grandes
const items = await prisma.pedido.findMany({
  where: {
    empresa_id: ctx.empresaId,
    // cursor: next_cursor passado pelo cliente
    ...(cursor ? { id: { lt: cursor } } : {}),
  },
  orderBy: { created_at: 'desc' },
  take: 50,
});

const nextCursor = items.length === 50 ? items[items.length - 1].id : null;
```

---

## 9. Auditoria e Rastreabilidade

### 9.1 O que auditar

| Ação                         | Audita Before | Audita After | Nível    |
|------------------------------|:-------------:|:------------:|:--------:|
| Login / Logout               | —             | metadata     | INFO     |
| Bloqueio de empresa          | status        | status       | WARN     |
| Criação de usuário           | —             | dados        | INFO     |
| Alteração de permissão       | perms antes   | perms depois | WARN     |
| Aprovação financeira         | status        | status       | INFO     |
| Emissão de NF                | —             | chave+status | INFO     |
| Exclusão (soft delete)       | objeto        | deleted_at   | WARN     |
| Tentativa de acesso negado   | —             | metadata     | SECURITY |
| Alteração de senha           | —             | metadata     | SECURITY |

### 9.2 Retenção de Logs

```
AuditLog → PostgreSQL (30 dias)
         → S3/Object Storage (1 ano)
         → Arquivamento (compliance: 5 anos)
```

---

## 10. Estratégia de Cache Redis

```typescript
// Estrutura de keys Redis
`session:{jti}`                          // dados da sessão, TTL 1h
`token:blacklist:{jti}`                  // token revogado, TTL = exp original
`refresh:blacklist:{refresh_jti}`        // refresh usado, TTL 7d
`tenant:softhouse:{softhouse_id}`        // dados da softhouse (ativo/bloqueado), TTL 5min
`tenant:empresa:{empresa_id}`            // dados da empresa, TTL 5min
`permissions:{user_id}`                  // permissions do usuário, TTL 10min
`dashboard:{empresa_id}:summary`         // KPIs do dashboard, TTL 5min
`estoque:{empresa_id}:posicao`           // posição de estoque, TTL 1min
`rate:limit:{ip}:{endpoint}`            // rate limiting por IP
`rate:limit:user:{user_id}:{endpoint}`  // rate limiting por usuário
```

---

## 11. Filas com RabbitMQ

```
Exchanges:
  erp.direct     → roteamento por routing key exata
  erp.topic      → roteamento por padrão
  erp.dlx        → dead letter exchange (falhas)

Filas:
  erp.nfe.emissao          → processamento assíncrono de NF
  erp.email.send           → envio de emails
  erp.relatorio.gerar      → geração de relatórios PDF/Excel
  erp.estoque.alertas      → alertas de estoque mínimo
  erp.financeiro.cobranca  → geração de boletos/PIX
  erp.webhook.enviar       → disparos de webhook para integrações
  erp.audit.persist        → persistência assíncrona de audit logs
```

```typescript
// Exemplo: emissão de NF de forma assíncrona
@Injectable()
export class NfeService {
  async emitirNfe(pedidoId: string): Promise<{ jobId: string }> {
    const job = await this.rabbitMq.publish('erp.nfe.emissao', {
      pedido_id: pedidoId,
      softhouse_id: this.tenantCtx.softouseId,
      empresa_id: this.tenantCtx.empresaId,
    });

    // Retorna imediatamente; NF é processada em background
    return { jobId: job.id };
  }
}

// Consumer: processa em worker separado
@RabbitSubscribe({ exchange: 'erp.direct', routingKey: 'nfe.emissao' })
async processarNfe(payload: NfeEmissaoJob): Promise<void> {
  // Sem contexto HTTP: setar manualmente o tenant context
  await tenantStorage.run({ softouseId: payload.softhouse_id, ... }, async () => {
    await this.sefazService.enviar(payload.pedido_id);
  });
}
```

---

## 12. Docker Compose e Deploy

```yaml
# docker-compose.yml
version: '3.9'

services:
  api:
    build:
      context: .
      dockerfile: apps/api/Dockerfile
    environment:
      - DATABASE_URL=postgresql://erp_user:${DB_PASSWORD}@postgres:5432/erp_saas
      - REDIS_URL=redis://:${REDIS_PASSWORD}@redis:6379
      - RABBITMQ_URL=amqp://erp_user:${RABBITMQ_PASSWORD}@rabbitmq:5672
      - JWT_SECRET=${JWT_SECRET}
      - JWT_REFRESH_SECRET=${JWT_REFRESH_SECRET}
      - NODE_ENV=production
    ports:
      - "3000:3000"
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
    deploy:
      replicas: 2
      resources:
        limits:
          memory: 512M

  worker:
    build:
      context: .
      dockerfile: apps/worker/Dockerfile
    environment:
      - DATABASE_URL=postgresql://erp_user:${DB_PASSWORD}@postgres:5432/erp_saas
      - REDIS_URL=redis://:${REDIS_PASSWORD}@redis:6379
      - RABBITMQ_URL=amqp://erp_user:${RABBITMQ_PASSWORD}@rabbitmq:5672
    depends_on:
      - rabbitmq
      - postgres

  web:
    build:
      context: .
      dockerfile: apps/web/Dockerfile
    environment:
      - NEXT_PUBLIC_API_URL=https://api.erp.com
    ports:
      - "3001:3001"

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: erp_saas
      POSTGRES_USER: erp_user
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts/init-rls.sql:/docker-entrypoint-initdb.d/01-rls.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U erp_user"]
      interval: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    command: redis-server --requirepass ${REDIS_PASSWORD} --maxmemory 256mb --maxmemory-policy allkeys-lru
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s

  rabbitmq:
    image: rabbitmq:3.13-management-alpine
    environment:
      RABBITMQ_DEFAULT_USER: erp_user
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASSWORD}
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    ports:
      - "15672:15672"   # Management UI (remover em produção)
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s

  nginx:
    image: nginx:alpine
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
    ports:
      - "80:80"
      - "443:443"
    depends_on:
      - api
      - web

volumes:
  postgres_data:
  redis_data:
  rabbitmq_data:
```

---

## 13. Seed Inicial

```typescript
// prisma/seed/index.ts
async function main() {
  // 1. Super Admin
  await prisma.superAdmin.upsert({
    where: { email: 'master@erp.com' },
    create: {
      email: 'master@erp.com',
      name: 'Super Admin',
      password: await bcrypt.hash('Master@2025!', 12),
    },
    update: {},
  });

  // 2. Planos
  const plans = [
    { name: 'Starter',    slug: 'starter',    price_monthly: 99,   max_empresas: 3,  max_users: 10  },
    { name: 'Pro',        slug: 'pro',         price_monthly: 299,  max_empresas: 15, max_users: 50  },
    { name: 'Enterprise', slug: 'enterprise',  price_monthly: 699,  max_empresas: 999, max_users: 999 },
  ];
  for (const plan of plans) {
    await prisma.plan.upsert({ where: { slug: plan.slug }, create: plan, update: plan });
  }

  // 3. Permissions — geradas automaticamente
  const resources = ['clientes', 'financeiro', 'estoque', 'vendas', 'pedidos', 'nfe', 'crm', 'usuarios', 'relatorios'];
  const actions = ['visualizar', 'criar', 'editar', 'excluir', 'aprovar', 'bloquear'];
  for (const resource of resources) {
    for (const action of actions) {
      await prisma.permission.upsert({
        where: { resource_action_scope: { resource, action, scope: null } },
        create: { resource, action },
        update: {},
      });
    }
  }

  // 4. Softhouse demo
  const softhouse = await prisma.softhouse.upsert({
    where: { slug: 'demo' },
    create: {
      name: 'Demo Softhouse',
      slug: 'demo',
      email: 'admin@demo.com',
      users: {
        create: {
          email: 'admin@demo.com',
          name: 'Admin Demo',
          password: await bcrypt.hash('Admin@2025!', 12),
          role: 'ADMIN',
        },
      },
    },
    update: {},
  });

  console.log('Seed concluído.');
}
```

---

## 14. Plano de Escalabilidade

### 14.1 Gargalos e Mitigações

| Gargalo                         | Impacto | Mitigação                                              |
|---------------------------------|:-------:|--------------------------------------------------------|
| Queries sem índice               | Alto    | Prisma query logging + pg_stat_statements              |
| N+1 em relacionamentos           | Alto    | Prisma `include` explícito; proibir lazy loading       |
| Emissão de NF síncrona          | Médio   | Fila RabbitMQ + polling de status                      |
| Cache miss em dashboard          | Médio   | Cache Redis + invalidação por evento                   |
| Conexões PostgreSQL excessivas   | Alto    | PgBouncer com pool por softhouse                       |
| Tenants com volumes discrepantes | Médio   | Rate limiting por plano; query timeout configurável    |

### 14.2 Monitoring Stack

```
Prometheus + Grafana → métricas de API, filas, banco
Sentry               → erros e performance
OpenTelemetry        → tracing distribuído
pg_stat_statements   → análise de queries lentas
Redis INFO           → hit rate e memória
```

### 14.3 Checklist de Segurança

- [x] Rate limiting por IP e por usuário (por plano)
- [x] JWT com expiração curta (1h) + Refresh Token com rotação
- [x] Blacklist de tokens no Redis
- [x] Bcrypt para senhas (custo 12)
- [x] RLS no PostgreSQL como camada adicional
- [x] Prisma middleware bloqueando queries sem tenant
- [x] HTTPS obrigatório (HSTS)
- [x] CORS restrito ao domínio da softhouse
- [x] Helmet.js (headers de segurança)
- [x] Sanitização de inputs (class-validator + class-transformer)
- [x] Certificados digitais criptografados em repouso (AES-256)
- [x] MFA opcional (TOTP via Google Authenticator)
- [x] Audit log de todas as operações de escrita
- [x] Soft delete (nada é apagado fisicamente)
- [x] Rotação de segredos via variáveis de ambiente (sem segredos no código)
```
