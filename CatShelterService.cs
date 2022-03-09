using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microservices.Common.Exceptions;
using Microservices.ExternalServices.Authorization;
using Microservices.ExternalServices.Authorization.Types;
using Microservices.ExternalServices.Billing;
using Microservices.ExternalServices.Billing.Types;
using Microservices.ExternalServices.CatDb;
using Microservices.ExternalServices.CatDb.Types;
using Microservices.ExternalServices.CatExchange;
using Microservices.ExternalServices.CatExchange.Types;
using Microservices.ExternalServices.Database;
using Microservices.Types;

namespace Microservices
{
    public class CatShelterService : ICatShelterService
    {
        private readonly IDatabase _db;
        private readonly IAuthorizationService _authorizationService;
        private readonly IBillingService _billingService;
        private readonly ICatInfoService _catInfoService;
        private readonly ICatExchangeService _catExchangeService;

        public CatShelterService(
            IDatabase database,
            IAuthorizationService authorizationService,
            IBillingService billingService,
            ICatInfoService catInfoService,
            ICatExchangeService catExchangeService)
        {
            _db = database;
            _authorizationService = authorizationService;
            _billingService = billingService;
            _catInfoService = catInfoService;
            _catExchangeService = catExchangeService;
        }

        public async Task<List<Cat>> GetCatsAsync(string sessionId, int skip, int limit, 
                                                  CancellationToken cancellationToken)
        {
            var user = await TryAuthorize(sessionId, cancellationToken);

            var catsInShelter = _db.GetCollection<CatEntity, Guid>("Cats");

            var billing = await TryTwice(() => _billingService.GetProductsAsync(skip, limit, cancellationToken));       

            var cats = new List<Cat>();
            foreach (var product in billing)
            {
                var catEntity = await TryTwice(() => catsInShelter.FindAsync(product.Id, cancellationToken));
                cats.Add(catEntity as Cat);
            }

            return cats;
        }

        public async Task AddCatToFavouritesAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            var user = await TryAuthorize(sessionId, cancellationToken);

            var userFavorites = await GetUserFavorites(user.UserId, cancellationToken);

            if (userFavorites == null)
            {
                userFavorites = new UserFavorites { Id = user.UserId, Favorites = new HashSet<Guid>() };
            }

            userFavorites.Favorites.Add(catId);

            await WriteUserFavorites(userFavorites, cancellationToken);
        }

        public async Task<List<Cat>> GetFavouriteCatsAsync(string sessionId, CancellationToken cancellationToken)
        {
            var user = await TryAuthorize(sessionId, cancellationToken);

            var userFavorites = await GetUserFavorites(user.UserId, cancellationToken);

            var catsInShelter = _db.GetCollection<CatEntity, Guid>("Cats");

            var cats = new List<Cat>();

            if (userFavorites != null && userFavorites.Favorites != null)
            {
                foreach (var id in userFavorites.Favorites)
                {
                    var cat = await TryTwice(() => _billingService.GetProductAsync(id, cancellationToken));
                    if (cat != null)
                    {
                        var catEntity = await TryTwice(() => catsInShelter.FindAsync(id, cancellationToken));
                        cats.Add(catEntity as Cat);
                    }
                }
            }

            return cats;
        }

        public async Task DeleteCatFromFavouritesAsync(string sessionId, Guid catId, 
                                                       CancellationToken cancellationToken)
        {
            var user = await TryAuthorize(sessionId, cancellationToken);

            var userFavorites = await GetUserFavorites(user.UserId, cancellationToken);

            if (userFavorites != null && userFavorites.Favorites != null)
            {
                userFavorites.Favorites.Remove(catId);
                await WriteUserFavorites(userFavorites, cancellationToken);
            }
        }

        public async Task<Bill> BuyCatAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            var user = await TryAuthorize(sessionId, cancellationToken);

            var product = await TryTwice(() => _billingService.GetProductAsync(catId, cancellationToken));

            if (product == null)
                throw new InvalidRequestException();

            var cat = await TryTwice(() => _db
                                            .GetCollection<CatEntity, Guid>("Cats")
                                            .FindAsync(catId, cancellationToken));

            return cat == null
                ? null    
                : await TryTwice(() => _billingService.SellProductAsync(catId, cat.Price, cancellationToken));
        }

        public async Task<Guid> AddCatAsync(string sessionId, AddCatRequest request, 
                                            CancellationToken cancellationToken)
        {
            var user = await TryAuthorize(sessionId, cancellationToken);

            var catInfo = await TryTwice(() => _catInfoService.FindByBreedNameAsync(request.Breed, cancellationToken));

            var priceInfo = await TryTwice(() => _catExchangeService.GetPriceInfoAsync(catInfo.BreedId, cancellationToken));

            var latestPrice = priceInfo.Prices != null && priceInfo.Prices.Count > 0
                ? priceInfo.Prices.Last()
                : new CatPriceInfo { Date = DateTime.Now, Price = 1000 };

            var cat = new Cat()
            {
                Id = Guid.NewGuid(),
                BreedId = catInfo.BreedId,
                AddedBy = user.UserId,
                Breed = request.Breed,
                Name = request.Name,
                CatPhoto = request.Photo,
                BreedPhoto = catInfo.Photo,
                Price = latestPrice.Price,
                Prices = priceInfo.Prices.Select(p => (p.Date, p.Price)).ToList()
            };

            await TryTwice(() => _billingService.AddProductAsync(
                new Product 
                { 
                    Id = cat.Id, 
                    BreedId = cat.BreedId 
                }, 
                cancellationToken
            ));

            await TryTwice(() => _db
                                    .GetCollection<CatEntity, Guid>("Cats")
                                    .WriteAsync(new CatEntity(cat), cancellationToken));

            return cat.Id;
        }

        private async Task<T> TryTwice<T>(Func<Task<T>> func)
        {
            try
            {
                return await func();
            }
            catch (ConnectionException)
            {
                try
                {
                    return await func();
                }
                catch(ConnectionException)
                {
                    throw new InternalErrorException();
                }
            }
        }

        private async Task TryTwice(Func<Task> func)
        {
            try
            {
                await func();
            }
            catch (ConnectionException)
            {
                try
                {
                    await func();
                }
                catch (ConnectionException)
                {
                    throw new InternalErrorException();
                }
            }
        }

        private class CatEntity : Cat, IEntityWithId<Guid>
        {
            public CatEntity(Cat cat)
            {
                Id = cat.Id;
                BreedId = cat.BreedId;
                AddedBy = cat.AddedBy;
                Breed = cat.Breed;
                Name = cat.Name;
                CatPhoto = cat.CatPhoto;
                BreedPhoto = cat.BreedPhoto;
                Price = cat.Price;
                Prices = cat.Prices;
            }
        }

        private class UserFavorites : IEntityWithId<Guid>
        {
            public Guid Id { get; set; }
            public HashSet<Guid> Favorites { get; set; }
        }

        private async Task<AuthorizationResult> TryAuthorize(string sessionId, CancellationToken cancellationToken)
        {
            var result = await TryTwice(() => _authorizationService.AuthorizeAsync(sessionId, cancellationToken));

            return result.IsSuccess
                ? result
                : throw new AuthorizationException();
        }

        private async Task<UserFavorites> GetUserFavorites(Guid userId, CancellationToken cancellationToken) =>
            await TryTwice(() => _db
                                    .GetCollection<UserFavorites, Guid>("Favorites")
                                    .FindAsync(userId, cancellationToken)
            );

        private async Task WriteUserFavorites(UserFavorites userFavorites, CancellationToken cancellationToken) =>
            await TryTwice(() => _db
                                    .GetCollection<UserFavorites, Guid>("Favorites")
                                    .WriteAsync(userFavorites, cancellationToken)
            );
    }
}