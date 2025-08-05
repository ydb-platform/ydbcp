package auth

import (
	"context"
	"errors"
	"fmt"

	"ydbcp/internal/util/xlog"
	"ydbcp/pkg/plugins/auth"

	"go.uber.org/zap"
)

type MockResourceID string
type MockContainerID string
type MockSubjectID string
type MockPermissionsList map[string]bool
type MockSubjectsPermissions map[MockSubjectID]MockPermissionsList
type MockContainer struct {
	Permissions MockSubjectsPermissions
}
type MockResource struct {
	ParentID    MockContainerID
	Permissions MockSubjectsPermissions
}
type MockTokenInfo struct {
	subject  MockSubjectID
	authCode auth.AuthCode
}
type MockAuthProvider struct {
	tokens     map[string]*MockTokenInfo
	containers map[MockContainerID]*MockContainer
	resources  map[MockResourceID]*MockResource
}

func NewMocResource(parent MockContainerID) *MockResource {
	return &MockResource{
		ParentID:    parent,
		Permissions: NewMockSubjectsPermissions(),
	}
}
func NewMocContainer() *MockContainer {
	return &MockContainer{
		Permissions: NewMockSubjectsPermissions(),
	}
}
func NewMockSubjectsPermissions() MockSubjectsPermissions {
	return make(map[MockSubjectID]MockPermissionsList)
}
func (sp MockSubjectsPermissions) AddSubjectPermission(subject string, permission string) {
	id := MockSubjectID(subject)
	if p, ok := sp[id]; ok {
		p[permission] = true
	}
	sp[id] = make(map[string]bool)
	sp[id][permission] = true
}
func (sp MockSubjectsPermissions) HasSubjectPermission(subject string, permission string) bool {
	if p, ok := sp[MockSubjectID(subject)]; ok {
		if _, ok = p[permission]; ok {
			return true
		}
	}
	return false
}
func (r *MockResource) AddSubjectPermission(subject string, permission string) {
	r.Permissions.AddSubjectPermission(subject, permission)
}
func (c *MockContainer) AddSubjectPermission(subject string, permission string) {
	c.Permissions.AddSubjectPermission(subject, permission)
}

type Option func(*MockAuthProvider)

func WithToken(token string, subject string, authCode auth.AuthCode) Option {
	return func(p *MockAuthProvider) {
		if p.tokens == nil {
			p.tokens = make(map[string]*MockTokenInfo)
		}
		p.tokens[token] = &MockTokenInfo{
			subject:  MockSubjectID(subject),
			authCode: authCode,
		}
	}
}

func WithContainer(id MockContainerID, container *MockContainer) Option {
	return func(p *MockAuthProvider) {
		if p.containers == nil {
			p.containers = make(map[MockContainerID]*MockContainer)
		}
		p.containers[id] = container
	}
}

func WithResource(id MockResourceID, res *MockResource) Option {
	return func(p *MockAuthProvider) {
		if p.resources == nil {
			p.resources = make(map[MockResourceID]*MockResource)
		}
		p.resources[id] = res
	}
}

func (p *MockAuthProvider) Init(ctx context.Context, config string) error {
	xlog.Info(
		ctx,
		"MockAuthProvider init",
		zap.String("config", config),
		zap.String("tokens", fmt.Sprintf("%v", p.tokens)),
		zap.String("containers", fmt.Sprintf("%v", p.containers)),
		zap.String("resources", fmt.Sprintf("%v", p.resources)),
	)
	return nil
}

func (p *MockAuthProvider) Finish(ctx context.Context) error {
	xlog.Info(ctx, "MockAuthProvider finish")
	return nil
}

func (p *MockAuthProvider) checkSubjectPermissionForContainer(subjectID, permission, containerID string) auth.AuthorizeResult {
	c, ok := p.containers[MockContainerID(containerID)]
	if !ok {
		return auth.AuthorizeResult{Code: auth.AuthCodeUnknownSubject}
	}
	if c.Permissions.HasSubjectPermission(subjectID, permission) {
		return auth.AuthorizeResult{Code: auth.AuthCodeSuccess}
	}
	return auth.AuthorizeResult{Code: auth.AuthCodePermissionDenied}
}

func (p *MockAuthProvider) checkSubjectPermissionForResource(subjectID, permission, resourceID string) auth.AuthorizeResult {
	res, ok := p.resources[MockResourceID(resourceID)]
	if !ok {
		return auth.AuthorizeResult{Code: auth.AuthCodeUnknownSubject}
	}
	if res.Permissions.HasSubjectPermission(subjectID, permission) {
		return auth.AuthorizeResult{Code: auth.AuthCodeSuccess}
	}
	return p.checkSubjectPermissionForContainer(subjectID, permission, string(res.ParentID))
}

func (p *MockAuthProvider) checkSubjectPermission(subjectID string, c auth.AuthorizeCheck) auth.AuthorizeResult {
	if len(c.ResourceID) == 0 {
		return p.checkSubjectPermissionForContainer(subjectID, c.Permission, c.ContainerID)
	}
	for _, resourceID := range c.ResourceID {
		result := p.checkSubjectPermissionForResource(subjectID, c.Permission, resourceID)
		if result.Code != auth.AuthCodeSuccess {
			return result
		}
	}
	return auth.AuthorizeResult{Code: auth.AuthCodePermissionDenied}
}

func (p *MockAuthProvider) Authorize(
	ctx context.Context,
	token string,
	checks ...auth.AuthorizeCheck,
) (results []auth.AuthorizeResult, subject string, err error) {
	xlog.Info(
		ctx,
		"MockAuthProvider Authorize",
		zap.String("AuthorizeChecks", fmt.Sprintf("%v", checks)),
	)
	if len(checks) == 0 {
		xlog.Error(ctx, "MockAuthProvider AuthorizeCheck list is empty")
		return nil, "", errors.New("AuthorizeCheck list is empty")
	}
	results = make([]auth.AuthorizeResult, 0, len(checks))

	info, ok := p.tokens[token]
	code := auth.AuthCodeInvalidToken
	if ok {
		code = info.authCode
	}
	if code != auth.AuthCodeSuccess {
		for range len(checks) {
			results = append(results, auth.AuthorizeResult{Code: code})
		}
		return results, "", nil
	}
	subject = string(info.subject)
	results = make([]auth.AuthorizeResult, 0, len(checks))
	for _, c := range checks {
		results = append(results, p.checkSubjectPermission(subject, c))
	}
	xlog.Info(
		ctx, "MockAuthProvider Authorize result",
		zap.String("AuthResults", fmt.Sprintf("%v", results)),
		zap.String("SubjectID", anonymousSubject),
	)
	return results, subject, nil
}

func NewMockAuthProvider(options ...Option) *MockAuthProvider {
	p := &MockAuthProvider{}
	for _, opt := range options {
		opt(p)
	}
	return p
}
